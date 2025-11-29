"""
attachment_service.py
========================

Fully centralized AttachmentService that handles ALL product-specific logic.
Target connectors remain completely product-agnostic.

Complete version with:
- Event loop fixes
- Zendesk API URL handling
- Enhanced error handling and logging
"""

import asyncio
import aiohttp
import logging
import base64
import time
import re
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs
import mimetypes
from concurrent.futures import ThreadPoolExecutor
import hashlib


@dataclass
class AttachmentMetadata:
    """Standardized attachment metadata format"""

    id: str
    name: str
    url: str
    content_type: Optional[str] = None
    size: Optional[int] = None
    source_type: Optional[str] = None
    auth_config: Optional[Dict] = None
    content: Optional[bytes] = None  # Pre-downloaded content


@dataclass
class ProcessedAttachment:
    """Result of processing an attachment"""

    name: str
    content: bytes
    content_type: str
    original_url: str
    processing_time: float
    source_type: str


@dataclass
class InlineImageResult:
    """Result of processing inline images"""

    updated_content: str
    extracted_attachments: List[ProcessedAttachment]
    failed_extractions: List[str]


class UniversalAttachmentService:
    """
    Centralized service that handles ALL attachment and inline image processing
    for ANY source platform. Target connectors remain completely product-agnostic.
    """

    def __init__(self, max_concurrent_downloads: int = 10, timeout_seconds: int = 30):
        """Initialize the universal attachment service"""
        self.max_concurrent_downloads = max_concurrent_downloads
        self.timeout_seconds = timeout_seconds
        self.session = None

        # ALL product-specific logic is centralized here
        self.source_patterns = self._initialize_source_patterns()
        self.auth_handlers = self._initialize_auth_handlers()
        self.url_builders = self._initialize_url_builders()

        # Performance tracking
        self.stats = {
            "total_downloads": 0,
            "successful_downloads": 0,
            "failed_downloads": 0,
            "total_bytes": 0,
            "download_times": [],
            "inline_images_processed": 0,
            "inline_images_extracted": 0,
        }

    def _detect_content_type_from_data(self, data: bytes) -> tuple:
        """
        Detect content type and extension from file data (magic bytes).
        Returns: (content_type, extension)
        """
        if not data or len(data) < 12:
            return "application/octet-stream", "bin"

        # Check magic bytes (file signatures)
        # JPEG
        if data[:2] == b"\xff\xd8":
            return "image/jpeg", "jpg"

        # PNG
        if data[:8] == b"\x89PNG\r\n\x1a\n":
            return "image/png", "png"

        # GIF
        if data[:6] in (b"GIF87a", b"GIF89a"):
            return "image/gif", "gif"

        # WebP
        if data[8:12] == b"WEBP":
            return "image/webp", "webp"

        # TIFF
        if data[:4] == b"II*\x00" or data[:4] == b"MM\x00*":
            return "image/tiff", "tiff"

        # BMP (less common)
        if data[:2] == b"BM":
            return "image/bmp", "bmp"

        # SVG
        if data[:5] == b"<?xml" or data[:4] == b"<svg":
            if b"<svg" in data[:1000]:
                return "image/svg+xml", "svg"

        # HEIC/HEIF (iPhone photos)
        if data[4:12] == b"ftypheic" or data[4:12] == b"ftypheix":
            return "image/heic", "heic"
        if data[4:12] == b"ftypheif" or data[4:12] == b"ftypmif1":
            return "image/heif", "heif"

        # PDF
        if data[:5] == b"%PDF-":
            return "application/pdf", "pdf"

        # ZIP (and Office documents)
        if data[:4] == b"PK\x03\x04":
            # Could be ZIP, DOCX, XLSX, PPTX
            return "application/zip", "zip"

        # Default
        return "application/octet-stream", "bin"

    def _get_final_content_type(self, detected_type: str, filename: str) -> str:
        """
        Get final content type with extension-based fallback.
        If detected type is generic, try to infer from filename extension.
        """
        import mimetypes

        # If detected type is specific (not generic), use it
        if detected_type and detected_type != "application/octet-stream":
            return detected_type

        # Detected type is generic - try to infer from filename extension
        if filename and "." in filename:
            inferred_type = mimetypes.guess_type(filename)[0]
            if inferred_type:
                logging.debug(
                    f"Inferred content type '{inferred_type}' from filename '{filename}'"
                )
                return inferred_type

        # Fallback to detected type (even if generic)
        return detected_type or "application/octet-stream"

    def _initialize_source_patterns(self) -> Dict[str, Dict]:
        """Initialize ALL source-specific patterns in one place"""
        return {
            # Zendesk patterns
            "zendesk_full": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.zendesk\.com/attachments/token/([^/?]+)[^"]*)"[^>]*?>',
                "groups": ["url", "token"],
                "source_type": "zendesk",
            },
            "zendesk_relative": {
                "pattern": r'<img[^>]*?src="(/attachments/([^"]+))"[^>]*?>',
                "groups": ["path", "attachment_info"],
                "source_type": "zendesk",
            },
            # Freshdesk patterns
            "freshdesk_full": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.(?:freshdesk\.com|attachments8\.freshdesk\.com)/[^"]*attachments[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "freshdesk",
            },
            "freshdesk_token": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.freshdesk\.com/attachments/token/([^/?]+)[^"]*)"[^>]*?>',
                "groups": ["url", "token"],
                "source_type": "freshdesk",
            },
            "freshdesk_relative": {
                "pattern": r'<img[^>]*?src="(/attachments/([^"]+))"[^>]*?>',
                "groups": ["path", "attachment_info"],
                "source_type": "freshdesk",
            },
            # Freshservice patterns
            "freshservice_full": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.freshservice\.com/[^"]*attachments[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "freshservice",
            },
            "freshservice_token": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.freshservice\.com/attachments/token/([^/?]+)[^"]*)"[^>]*?>',
                "groups": ["url", "token"],
                "source_type": "freshservice",
            },
            "freshdesk_relative": {
                "pattern": r'<img[^>]*?src="(/attachments/([^"]+))"[^>]*?>',
                "groups": ["path", "attachment_info"],
                "source_type": "freshdesk",
            },
            # Freshdesk S3 Storage patterns (Freshchat and newer Freshdesk)
            "freshdesk_s3_bucket": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.s3\.amazonaws\.com/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "freshdesk",
            },
            "freshdesk_s3_cdn": {
                "pattern": r'<img[^>]*?src="(https://s3\.amazonaws\.com/cdn\.freshdesk\.com/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "freshdesk",
            },
            # Freshdesk inline attachment token URLs (JWT-based, will redirect to actual file)
            "freshdesk_inline_token": {
                "pattern": r'<img[^>]*?src="(https://attachment\.freshdesk\.com/inline/attachment\?token=[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "freshdesk",
            },
            # ServiceNow patterns
            "servicenow_full": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.service-now\.com/[^"]*attachment[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "servicenow",
            },
            "servicenow_api": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.service-now\.com/api/[^"]*attachment[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "servicenow",
            },
            # Azure Blob Storage patterns
            "azure_blob": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.blob\.core\.windows\.net/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "azure_blob",
            },
            "azure_blob_saas": {
                "pattern": r'<img[^>]*?src="(https://saasgeniestorage\.blob\.core\.windows\.net/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "azure_blob",
            },
            # Jira/Atlassian patterns
            "jira_full": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.atlassian\.net/[^"]*attachments?[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "jira",
            },
            "jira_secure": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.atlassian\.net/secure/attachment/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "jira",
            },
            # Office 365/SharePoint patterns
            "sharepoint": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.sharepoint\.com/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "sharepoint",
            },
            # Google Drive patterns
            "gdrive": {
                "pattern": r'<img[^>]*?src="(https://drive\.google\.com/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "gdrive",
            },
            # Generic patterns for other systems
            "generic_attachment": {
                "pattern": r'<img[^>]*?src="(https://[^"]+/[^"]*(?:attachment|file|download)[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
            # Generic S3 patterns (catches S3 URLs from any source)
            "generic_s3_bucket": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.s3\.amazonaws\.com/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
            "generic_s3_regional": {
                "pattern": r'<img[^>]*?src="(https://s3-[^.]+\.amazonaws\.com/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
            "generic_s3_path": {
                "pattern": r'<img[^>]*?src="(https://s3\.amazonaws\.com/[^"]+/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
            "generic_cloudfront": {
                "pattern": r'<img[^>]*?src="(https://[^.]+\.cloudfront\.net/[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
            "generic_attachment_token": {
                "pattern": r'<img[^>]*?src="(https://[^"]+/inline/attachment\?token=[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
            "generic_token_redirect": {
                "pattern": r'<img[^>]*?src="(https://[^"]+/[^"]*\?token=[^"]*)"[^>]*?>',
                "groups": ["url"],
                "source_type": "generic",
            },
        }

    def _initialize_auth_handlers(self) -> Dict[str, callable]:
        """Initialize ALL source-specific authentication handlers"""
        return {
            "zendesk": self._auth_zendesk,
            "freshdesk": self._auth_freshdesk,
            "freshservice": self._auth_freshservice,
            "servicenow": self._auth_servicenow,
            "jira": self._auth_jira,
            "atlassian": self._auth_jira,  # Alias
            "sharepoint": self._auth_sharepoint,
            "gdrive": self._auth_gdrive,
            "azure_blob": self._auth_azure_blob,
            "generic": self._auth_generic,
        }

    def _initialize_url_builders(self) -> Dict[str, callable]:
        """Initialize ALL source-specific URL builders for relative paths"""
        return {
            "zendesk": self._build_zendesk_url,
            "freshdesk": self._build_freshdesk_url,
            "freshservice": self._build_freshservice_url,
            "servicenow": self._build_servicenow_url,
            "jira": self._build_jira_url,
            "generic": self._build_generic_url,
        }

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=self.max_concurrent_downloads,
            ttl_dns_cache=300,
            use_dns_cache=True,
            force_close=True,  # Force close connections to avoid reuse issues
        )
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "Universal-Migration-Service/1.0"},
        )
        logging.info("Initialized UniversalAttachmentService session")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            # Wait a bit for the underlying connections to close
            await asyncio.sleep(0.25)
            logging.info("Closed UniversalAttachmentService session")

    def detect_source_type(self, url: str) -> str:
        """Auto-detect source platform from URL - ENHANCED"""
        url_lower = url.lower()

        detection_patterns = [
            (
                "azure_blob",
                ["blob.core.windows.net", "saasgeniestorage.blob.core.windows.net"],
            ),
            (
                "servicenow",
                [
                    "service-now.com/api/now/attachment",
                    "servicenow.com/api/now/attachment",
                ],
            ),
            ("freshdesk", ["freshdesk.com", "attachments8.freshdesk.com"]),
            ("zendesk", ["zendesk.com"]),
            ("freshservice", ["freshservice.com"]),
            ("jira", ["atlassian.net", "jira.com"]),
            ("sharepoint", ["sharepoint.com"]),
            ("gdrive", ["drive.google.com", "googleapis.com"]),
        ]

        for source_type, patterns in detection_patterns:
            if any(pattern in url_lower for pattern in patterns):
                logging.info(
                    f"Detected source type '{source_type}' from URL: {url[:100]}..."
                )
                return source_type

        return "generic"

    async def _handle_zendesk_attachment_url(
        self, url: str, auth_config: Optional[Dict]
    ) -> Optional[str]:
        """
        Handle Zendesk attachment URLs.
        Zendesk provides API URLs that return JSON metadata, not the actual file.
        We need to fetch the metadata first to get the actual content_url.
        """
        if "zendesk.com/api/v2/attachments/" in url and url.endswith(".json"):
            logging.info(f"Detected Zendesk API URL, fetching metadata first: {url}")

            # Get auth headers for API call
            headers = self._auth_zendesk(url, auth_config)

            try:
                if not self.session:
                    logging.error("Session not initialized for Zendesk API call")
                    return None

                async with self.session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        attachment = data.get("attachment", {})
                        content_url = attachment.get("content_url")

                        if content_url:
                            logging.info(f"Found content URL: {content_url}")
                            return content_url
                        else:
                            logging.error(
                                f"No content_url found in Zendesk attachment metadata: {data}"
                            )
                            return None
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Failed to fetch Zendesk attachment metadata: {response.status} - {error_text}"
                        )
                        return None
            except Exception as e:
                logging.error(f"Error fetching Zendesk attachment metadata: {e}")
                import traceback

                logging.debug(f"Traceback: {traceback.format_exc()}")
                return None

        # Not a Zendesk API URL, return as-is
        return url

    async def process_inline_images_universal(
        self,
        content: str,
        source_domain: Optional[str] = None,
        auth_config: Optional[Dict] = None,
    ) -> InlineImageResult:
        """
        Universal inline image processor that works with ANY source platform

        Args:
            content: HTML content containing inline images
            source_domain: Domain for relative URL resolution
            auth_config: Authentication configuration

        Returns:
            InlineImageResult with updated content and extracted attachments
        """
        extracted_attachments = []
        failed_extractions = []
        updated_content = content

        # Process all patterns to extract inline images
        inline_images = self._extract_all_inline_images(content, source_domain)

        if not inline_images:
            return InlineImageResult(updated_content, [], [])

        logging.info(f"Found {len(inline_images)} inline images to process")
        self.stats["inline_images_processed"] += len(inline_images)

        # Download all inline images concurrently
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        download_tasks = [
            self._download_inline_image(img, auth_config, semaphore)
            for img in inline_images
        ]

        results = await asyncio.gather(*download_tasks, return_exceptions=True)

        # Process results and update content
        for i, (result, img_info) in enumerate(zip(results, inline_images)):
            if isinstance(result, Exception):
                logging.error(
                    f"Failed to download inline image {img_info['url']}: {result}"
                )
                failed_extractions.append(img_info["url"])
            elif result:
                extracted_attachments.append(result)
                self.stats["inline_images_extracted"] += 1

                # Remove the inline image from content
                updated_content = self._remove_inline_image_from_content(
                    updated_content, img_info
                )
            else:
                failed_extractions.append(img_info["url"])

        return InlineImageResult(
            updated_content, extracted_attachments, failed_extractions
        )

    def _extract_all_inline_images(
        self, content: str, source_domain: Optional[str] = None
    ) -> List[Dict]:
        """Extract inline images using ALL source patterns"""
        inline_images = []

        for pattern_name, pattern_config in self.source_patterns.items():
            try:
                pattern = re.compile(pattern_config["pattern"], re.IGNORECASE)
                matches = pattern.findall(content)

                for match in matches:
                    img_info = self._process_pattern_match(
                        match, pattern_config, source_domain
                    )
                    if img_info:
                        inline_images.append(img_info)

            except Exception as e:
                logging.error(f"Error processing pattern {pattern_name}: {e}")

        # Remove duplicates based on URL
        seen_urls = set()
        unique_images = []
        for img in inline_images:
            if img["url"] not in seen_urls:
                seen_urls.add(img["url"])
                unique_images.append(img)

        return unique_images

    def _process_pattern_match(
        self,
        match: Union[str, Tuple],
        pattern_config: Dict,
        source_domain: Optional[str] = None,
    ) -> Optional[Dict]:
        """Process a regex match into standardized image info"""
        try:
            groups = pattern_config["groups"]
            source_type = pattern_config["source_type"]

            if isinstance(match, tuple):
                if len(groups) == 1:
                    url = match[0]
                    token = None
                    path = None
                elif len(groups) == 2:
                    if "url" in groups and "token" in groups:
                        url, token = match
                        path = None
                    elif "path" in groups:
                        path, attachment_info = match
                        url = self._build_url_from_path(
                            path, source_type, source_domain
                        )
                        token = None
                    else:
                        url = match[0]
                        token = match[1] if len(match) > 1 else None
                        path = None
                else:
                    url = match[0]
                    token = None
                    path = None
            else:
                url = match
                token = None
                path = None

            if not url:
                return None

            # Generate filename
            filename = self._generate_filename(url, token, source_type)

            return {
                "url": url,
                "token": token,
                "path": path,
                "source_type": source_type,
                "filename": filename,
                "original_match": match,
            }

        except Exception as e:
            logging.error(f"Error processing pattern match: {e}")
            return None

    def _build_url_from_path(
        self, path: str, source_type: str, source_domain: Optional[str] = None
    ) -> Optional[str]:
        """Build full URL from relative path"""
        if not path or not source_domain:
            return None

        url_builder = self.url_builders.get(source_type, self.url_builders["generic"])
        return url_builder(path, source_domain)

    def _generate_filename(
        self, url: str, token: Optional[str], source_type: str, content: bytes = None
    ) -> str:
        """Generate appropriate filename for attachment"""
        if token:
            return f"{source_type}_attachment_{token[:10]}.png"

        # Try to extract filename from URL
        try:
            from urllib.parse import urlparse, unquote

            parsed = urlparse(url)
            path_parts = parsed.path.split("/")

            # Look for actual filename
            for part in reversed(path_parts):
                if "." in part and len(part) > 1:
                    filename = unquote(part)
                    # If we have content, verify the extension matches
                    if content:
                        detected_type, detected_ext = (
                            self._detect_content_type_from_data(content)
                        )
                        file_base = filename.rsplit(".", 1)[0]
                        return f"{file_base}.{detected_ext}"
                    return filename

            # Check query params for filename
            query_params = parse_qs(parsed.query)
            if "name" in query_params:
                return query_params["name"][0]

        except:
            pass

        # Generate filename with correct extension from content
        if content:
            detected_type, detected_ext = self._detect_content_type_from_data(content)
            if token:
                return f"{source_type}_attachment_{token[:10]}.{detected_ext}"
            else:
                import hashlib

                url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
                return f"{source_type}_attachment_{url_hash}.{detected_ext}"

        # Fallback if no content available
        if token:
            return f"{source_type}_attachment_{token[:10]}.bin"

        import hashlib

        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"{source_type}_attachment_{url_hash}.bin"

    async def _download_inline_image(
        self, img_info: Dict, auth_config: Optional[Dict], semaphore: asyncio.Semaphore
    ) -> Optional[ProcessedAttachment]:
        """Download a single inline image"""
        async with semaphore:
            try:
                source_type = img_info["source_type"]
                url = img_info["url"]

                # Handle Zendesk API URLs
                if source_type == "zendesk":
                    url = await self._handle_zendesk_attachment_url(url, auth_config)
                    if not url:
                        return None

                # Get auth headers
                auth_handler = self.auth_handlers.get(
                    source_type, self.auth_handlers["generic"]
                )
                headers = auth_handler(url, auth_config)

                # Download content
                content = await self._make_download_request(url, headers)
                if not content:
                    return None

                # Detect content type from actual file data
                detected_content_type, detected_ext = (
                    self._detect_content_type_from_data(content)
                )

                # Generate filename with correct extension
                filename = self._generate_filename(
                    url, img_info.get("token"), source_type, content
                )

                # Get final content type with extension-based fallback
                final_content_type = self._get_final_content_type(
                    detected_content_type, filename
                )

                logging.info(
                    f"Detected: {detected_content_type} for {filename}, final: {final_content_type}"
                )

                return ProcessedAttachment(
                    name=filename,
                    content=content,
                    content_type=final_content_type,  # ✅ NEW
                    original_url=url,
                    processing_time=0.0,
                    source_type=source_type,
                )

            except Exception as e:
                logging.error(
                    f"Error downloading inline image {img_info.get('url', 'unknown')}: {e}"
                )
                return None

    def _remove_inline_image_from_content(self, content: str, img_info: Dict) -> str:
        """Remove inline image reference from content"""
        try:
            url = img_info["url"]
            # Create pattern to match the specific img tag
            pattern = re.compile(
                r'<img[^>]*?src=["\']' + re.escape(url) + r'["\'][^>]*?/?>',
                re.IGNORECASE,
            )
            return pattern.sub("", content)
        except Exception as e:
            logging.error(f"Error removing inline image from content: {e}")
            return content

    async def download_attachments_batch(
        self,
        attachments: List[Union[Dict, AttachmentMetadata]],
        auth_config: Optional[Dict] = None,
    ) -> List[Optional[ProcessedAttachment]]:
        """Download multiple attachments concurrently"""
        if not attachments:
            logging.info("No attachments to download")
            return []

        logging.info(f"Starting batch download of {len(attachments)} attachments")

        # Normalize attachment metadata
        normalized_attachments = []
        for i, att in enumerate(attachments):
            if isinstance(att, dict):
                # Handle various URL field names
                # swapped the order of content_url and url to prioritize content_url
                url = (
                    att.get("content_url")
                    or att.get("url")
                    or att.get("attachment_url")
                    or att.get("mapped_content_url")
                )
                if not url:
                    logging.error(f"No URL found in attachment {i}: {att}")
                    continue

                # Handle various name field names
                name = (
                    att.get("name")
                    or att.get("file_name")
                    or att.get("filename")
                    or f"attachment_{i}"
                )

                metadata = AttachmentMetadata(
                    id=str(att.get("id", att.get("attachment_id", f"unknown_{i}"))),
                    name=name,
                    url=url,
                    content_type=att.get("content_type"),
                    size=att.get("size"),
                    source_type=att.get("source_type"),
                    # auth_config=att.get('auth_config', auth_config),
                    auth_config=auth_config,
                    content=att.get("content")  # Include pre-downloaded content if available
                )

                logging.debug(f"Normalized attachment {i}: {name} from {url[:100]}...")
            else:
                metadata = att
                # if not metadata.auth_config:
                #     metadata.auth_config = auth_config
                metadata.auth_config = auth_config

            normalized_attachments.append(metadata)

        # Create download semaphore
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

        # Create download tasks
        download_tasks = [
            self._download_single_attachment(attachment, semaphore)
            for attachment in normalized_attachments
        ]

        # Execute downloads concurrently
        results = await asyncio.gather(*download_tasks, return_exceptions=True)

        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(
                    f"Download failed for {normalized_attachments[i].name}: {result}"
                )
                processed_results.append(None)
                self.stats["failed_downloads"] += 1
            else:
                processed_results.append(result)
                if result:
                    self.stats["successful_downloads"] += 1
                    self.stats["total_bytes"] += len(result.content)
                    logging.info(
                        f"Successfully downloaded {result.name} ({len(result.content)} bytes)"
                    )
                else:
                    self.stats["failed_downloads"] += 1
                    logging.error(
                        f"Failed to download {normalized_attachments[i].name}"
                    )

        self.stats["total_downloads"] += len(attachments)

        # Log batch statistics
        successful_count = len([r for r in processed_results if r])
        logging.info(
            f"Batch download complete: {successful_count}/{len(attachments)} successful"
        )

        return processed_results

    async def _download_single_attachment(
        self, attachment: AttachmentMetadata, semaphore: asyncio.Semaphore
    ) -> Optional[ProcessedAttachment]:
        """Download a single attachment with semaphore control"""
        start_time = time.time()

        async with semaphore:
            try:
                # Check if content is already available (pre-downloaded)
                if attachment.content and isinstance(attachment.content, bytes) and len(attachment.content) > 0:
                    logging.info(f"Using pre-downloaded content for {attachment.name} ({len(attachment.content)} bytes)")
                    
                    # Detect content type from existing content
                    detected_content_type, detected_ext = self._detect_content_type_from_data(attachment.content)
                    content_type = attachment.content_type or detected_content_type
                    final_content_type = self._get_final_content_type(content_type, attachment.name)
                    
                    processing_time = time.time() - start_time
                    self.stats["download_times"].append(processing_time)
                    
                    return ProcessedAttachment(
                        name=attachment.name,
                        content=attachment.content,
                        content_type=final_content_type,
                        original_url=attachment.url,
                        processing_time=processing_time,
                        source_type=attachment.source_type or "pre_downloaded",
                    )

                # Auto-detect source type if not provided
                source_type = attachment.source_type or self.detect_source_type(
                    attachment.url
                )

                logging.debug(
                    f"Downloading {attachment.name} from {source_type} platform"
                )

                # Add specific debugging for Azure Blob Storage
                if "blob.core.windows.net" in attachment.url:
                    logging.debug(
                        f"AZURE BLOB: Downloading from Azure Blob Storage: {attachment.name}"
                    )
                    logging.debug(f"AZURE BLOB: URL: {attachment.url}")

                # Handle ServiceNow session establishment
                if source_type == "servicenow":
                    await self._ensure_servicenow_session(attachment.auth_config)

                # Handle Zendesk API URLs
                actual_url = attachment.url
                if source_type == "zendesk":
                    actual_url = await self._handle_zendesk_attachment_url(
                        attachment.url, attachment.auth_config
                    )
                    if not actual_url:
                        logging.error(
                            f"Could not resolve Zendesk attachment URL for {attachment.name}"
                        )
                        return None

                # Get auth headers
                auth_handler = self.auth_handlers.get(
                    source_type, self.auth_handlers["generic"]
                )
                headers = auth_handler(actual_url, attachment.auth_config)

                # Add Azure Blob specific debugging for auth headers
                if "blob.core.windows.net" in attachment.url:
                    if not headers:
                        logging.debug(
                            "AZURE BLOB: No auth headers (may be pre-signed URL)"
                        )
                    else:
                        logging.debug(
                            f"AZURE BLOB: Auth headers: {list(headers.keys())}"
                        )

                # Log auth info for debugging
                if headers:
                    logging.debug(
                        f"Using auth headers for {attachment.name}: {list(headers.keys())}"
                    )
                else:
                    logging.debug(
                        f"No auth headers for {attachment.name} (may be pre-signed URL)"
                    )

                # Download content
                content = await self._make_download_request(actual_url, headers)
                if not content:
                    logging.error(f"No content received for {attachment.name}")
                    return None

                # Detect content type from downloaded content
                detected_content_type, detected_ext = (
                    self._detect_content_type_from_data(content)
                )

                # Start with source content_type, fallback to detected
                content_type = attachment.content_type or detected_content_type

                # Get final content type with extension-based fallback
                final_content_type = self._get_final_content_type(
                    content_type, attachment.name
                )

                processing_time = time.time() - start_time
                self.stats["download_times"].append(processing_time)

                return ProcessedAttachment(
                    name=attachment.name,
                    content=content,
                    content_type=final_content_type,  # ✅ NEW
                    original_url=attachment.url,
                    processing_time=processing_time,
                    source_type=source_type,
                )

            except Exception as e:
                logging.error(f"Error downloading {attachment.name}: {e}")
                import traceback

                logging.debug(f"Traceback: {traceback.format_exc()}")
                return None

    async def _make_download_request(self, url: str, headers: Dict) -> Optional[bytes]:
        """Make the actual HTTP request to download attachment"""
        try:
            logging.debug(f"Making request to: {url[:100]}...")

            # Check if this is an Azure Blob Storage request with SDK client
            if "azure_blob_client" in headers:
                blob_client = headers["azure_blob_client"]
                try:
                    # Use Azure SDK for authenticated download
                    logging.debug("Using Azure SDK for authenticated blob download")
                    blob_data = blob_client.download_blob()
                    content = blob_data.readall()
                    logging.info(
                        f"Downloaded {len(content)} bytes from Azure Blob Storage using SDK"
                    )
                    return content
                except Exception as e:
                    logging.warning(
                        f"Azure SDK download failed: {e}, falling back to HTTP"
                    )
                    # Remove the azure_blob_client from headers and continue with HTTP
                    headers = {
                        k: v for k, v in headers.items() if k != "azure_blob_client"
                    }

            if not self.session:
                logging.error(
                    "Session not initialized! Make sure to use async context manager."
                )
                return None

            async with self.session.get(
                url, headers=headers, allow_redirects=True
            ) as response:
                logging.debug(f"Response status: {response.status}")

                # Handle rate limiting
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logging.warning(f"Rate limited on {url}, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)

                    # Retry once
                    async with self.session.get(
                        url, headers=headers, allow_redirects=True
                    ) as retry_response:
                        retry_response.raise_for_status()
                        content = await retry_response.read()
                        logging.info(
                            f"Downloaded {len(content)} bytes from {url[:100]}... (after retry)"
                        )
                        return content

                # Handle successful response
                elif response.status == 200:
                    content = await response.read()
                    logging.info(f"Downloaded {len(content)} bytes from {url[:100]}...")
                    return content

                # Handle redirects
                elif response.status in [301, 302, 303, 307, 308]:
                    logging.info(f"Redirect {response.status} for {url[:100]}...")
                    # aiohttp should handle this automatically with allow_redirects=True
                    return None

                # Handle other errors
                else:
                    error_text = await response.text()
                    logging.error(
                        f"HTTP {response.status} for {url[:100]}...: {error_text[:200]}"
                    )
                    response.raise_for_status()

        except aiohttp.ClientError as e:
            logging.error(f"HTTP error downloading from {url[:100]}...: {e}")
            return None
        except asyncio.TimeoutError:
            logging.error(f"Timeout downloading from {url[:100]}...")
            return None
        except Exception as e:
            logging.error(f"Unexpected error downloading from {url[:100]}...: {e}")
            import traceback

            logging.debug(f"Traceback: {traceback.format_exc()}")
            return None

    async def _ensure_servicenow_session(self, auth_config: Optional[Dict]) -> None:
        """Establish ServiceNow session by making a table API call first"""
        if not auth_config or not self.session:
            return

        # Extract domain from auth_config or try to determine it
        source_domain = auth_config.get("source_domain")
        if not source_domain:
            logging.warning(
                "No ServiceNow domain found in auth_config, skipping session establishment"
            )
            return

        try:
            # Make a simple table API call to establish session
            session_url = f"https://{source_domain}.service-now.com/api/now/table/incident?sysparm_limit=1"
            headers = self._auth_servicenow(session_url, auth_config)

            logging.debug(f"Establishing ServiceNow session via: {session_url}")

            async with self.session.get(session_url, headers=headers) as response:
                if response.status == 200:
                    logging.debug("ServiceNow session established successfully")
                    # The session cookies are now automatically stored in self.session
                else:
                    error_text = await response.text()
                    logging.warning(
                        f"Failed to establish ServiceNow session: {response.status} - {error_text[:200]}"
                    )

        except Exception as e:
            logging.warning(f"Error establishing ServiceNow session: {e}")
            # Continue anyway, the attachment download might still work

    # ===================================================================
    # SOURCE-SPECIFIC AUTHENTICATION HANDLERS (ALL IN ONE PLACE)
    # ===================================================================

    def _auth_zendesk(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """
        Zendesk authentication handler.
        Handles both pre-signed URLs and URLs requiring authentication.
        """
        headers = {}

        # Check if this is a pre-signed URL with authentication token
        if "/attachments/token/" in url:
            # Token-based URL, no additional auth needed
            logging.debug(f"Zendesk token-based URL detected, no auth headers needed")
            return headers

        if auth_config:
            # Use pre-encoded api_key if available (already base64 encoded)
            api_key = auth_config.get("api_key")
            if api_key:
                headers["Authorization"] = f"Basic {api_key}"
                logging.debug(f"Using pre-encoded Zendesk API key")
            else:
                # Build auth from email and token
                email = auth_config.get("email")
                api_token = auth_config.get("api_token")

                if email and api_token:
                    # Zendesk expects email/token:token format
                    credentials = base64.b64encode(
                        f"{email}/token:{api_token}".encode()
                    ).decode()
                    headers["Authorization"] = f"Basic {credentials}"
                    logging.debug(f"Built Zendesk auth from email and token")
                else:
                    # Try username/password format (for backward compatibility)
                    username = auth_config.get("username")
                    password = auth_config.get("password")
                    if username and password:
                        credentials = base64.b64encode(
                            f"{username}:{password}".encode()
                        ).decode()
                        headers["Authorization"] = f"Basic {credentials}"
                        logging.debug(f"Built Zendesk auth from username/password")
                    else:
                        logging.debug(
                            f"No valid Zendesk authentication found in config"
                        )
        else:
            logging.debug(f"No auth config provided for Zendesk")

        return headers

    def _auth_freshdesk(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """Freshdesk authentication handler"""
        headers = {}
        # Freshdesk signed URLs typically don't need additional auth
        is_presigned_url = "attachments8.freshdesk.com" in url or "X-Amz-Algorithm" in url
        if auth_config and not is_presigned_url:
            api_key = auth_config.get("api_key")
            if api_key:
                headers["Authorization"] = f"Basic {api_key}"
        return headers

    def _auth_freshservice(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """Freshservice authentication handler"""
        headers = {}
        if auth_config and "token" not in url.lower():
            api_key = auth_config.get("api_key")
            if api_key:
                headers["Authorization"] = f"Basic {api_key}"
        return headers

    def _auth_servicenow(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """ServiceNow authentication handler - ENHANCED"""
        headers = {}

        # HARDCODED FOR TESTING - Remove this block when done testing
        # if True:  # Always use hardcoded creds for testing
        #     import base64
        #     username = "admin"
        #     password = "Rz@/tWM3Q3jx"
        #     credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        #     headers['Authorization'] = f'Basic {credentials}'
        #     logging.info("Using HARDCODED ServiceNow authentication for testing")
        #     return headers

        # Original dynamic logic (will be used after testing)
        if auth_config:
            # Try OAuth first
            oauth_token = auth_config.get("oauth_token")
            if oauth_token:
                headers["Authorization"] = f"Bearer {oauth_token}"
                logging.info("Using ServiceNow OAuth authentication")
            else:
                # Fall back to basic auth
                username = auth_config.get("username") or auth_config.get(
                    "source_username"
                )
                password = auth_config.get("password") or auth_config.get(
                    "source_password"
                )

                if username == "x-sn-apikey" and password:
                    # Use API key authentication
                    headers["x-sn-apikey"] = password
                    logging.info("Using ServiceNow API key authentication")
                elif username and password:
                    import base64

                    credentials = base64.b64encode(
                        f"{username}:{password}".encode()
                    ).decode()
                    headers["Authorization"] = f"Basic {credentials}"
                    logging.info("Using ServiceNow basic authentication")
                else:
                    # Try pre-encoded API key
                    api_key = auth_config.get("api_key")
                    if api_key:
                        headers["Authorization"] = f"Basic {api_key}"
                        logging.info("Using ServiceNow pre-encoded API key")
                    else:
                        logging.warning("No valid ServiceNow authentication found")
        else:
            logging.warning("No auth config provided for ServiceNow")

        # Add required headers for ServiceNow attachment API
        headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )

        return headers

    def _auth_jira(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """Jira/Atlassian authentication handler"""
        headers = {}
        if auth_config:
            bearer_token = auth_config.get("bearer_token") or auth_config.get(
                "access_token"
            )
            if bearer_token:
                headers["Authorization"] = f"Bearer {bearer_token}"
            else:
                api_key = auth_config.get("api_key")
                email = auth_config.get("email")
                api_token = auth_config.get("api_token")

                if email and api_token:
                    credentials = base64.b64encode(
                        f"{email}:{api_token}".encode()
                    ).decode()
                    headers["Authorization"] = f"Basic {credentials}"
                elif api_key:
                    headers["Authorization"] = f"Basic {api_key}"
        return headers

    def _auth_sharepoint(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """SharePoint authentication handler"""
        headers = {}
        if auth_config:
            access_token = auth_config.get("access_token")
            if access_token:
                headers["Authorization"] = f"Bearer {access_token}"
        return headers

    def _auth_gdrive(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """Google Drive authentication handler"""
        headers = {}
        if auth_config:
            access_token = auth_config.get("access_token")
            if access_token:
                headers["Authorization"] = f"Bearer {access_token}"
        return headers

    def _auth_azure_blob(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """Azure Blob Storage authentication handler - uses Azure SDK with connection string"""
        headers = {}

        try:
            # Import Azure SDK components
            from azure.storage.blob import BlobServiceClient
            from src.utils.config import CONFIG
            import os
            from urllib.parse import urlparse

            # Get Azure connection string from environment/config
            connection_string = (
                os.getenv("AZURE_STORAGE_CONNECTION_STRING")
                or os.getenv("AZURE_CONNECTION_STRING")
                or CONFIG.get("AZURE_STORAGE_CONNECTION_STRING")
                or CONFIG.get("AZURE_CONNECTION_STRING")
            )

            if not connection_string:
                logging.debug(
                    "No Azure connection string found, assuming pre-signed URL"
                )
                return headers

            # Parse the URL to get container and blob path
            parsed = urlparse(url)
            path_parts = parsed.path.lstrip("/").split("/")

            if len(path_parts) < 2:
                logging.debug("Invalid Azure Blob URL format")
                return headers

            container_name = path_parts[0]
            blob_name = "/".join(path_parts[1:])

            logging.debug(f"Azure Blob: container={container_name}, blob={blob_name}")

            # Create Azure Blob Service Client
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )

            # Check if blob exists
            if blob_client.exists():
                logging.debug("Azure blob exists, using authenticated download")
                # Store the blob client for use in download
                # Note: We'll handle the actual download in the calling code
                return {"azure_blob_client": blob_client}
            else:
                logging.debug(
                    "Azure blob not found with credentials, falling back to direct URL"
                )
                return headers

        except ImportError:
            logging.debug("Azure SDK not available, assuming pre-signed URL")
            return headers
        except Exception as e:
            logging.debug(
                f"Azure authentication setup failed: {e}, falling back to direct URL"
            )
            return headers

        return headers

    def _auth_generic(self, url: str, auth_config: Optional[Dict]) -> Dict:
        """Generic authentication handler"""
        headers = {}
        if auth_config:
            api_key = auth_config.get("api_key")
            bearer_token = auth_config.get("bearer_token")

            if bearer_token:
                headers["Authorization"] = f"Bearer {bearer_token}"
            elif api_key:
                headers["Authorization"] = f"Basic {api_key}"
        return headers

    # Addiontional product specific methods

    def normalize_attachment_data(
        self, attachments: List[Dict], headers_dict: Dict
    ) -> Tuple[List[Dict], Optional[Dict]]:
        """
        Normalize attachment data and extract authentication from various sources.
        This centralizes ALL source-specific logic in the attachment service.

        Args:
            attachments: Raw attachment data from any source
            headers_dict: Headers that might contain auth info

        Returns:
            Tuple of (normalized_attachments, auth_config)
        """
        normalized_attachments = []
        auth_config = None
        source_domain = None

        # First, detect source type from attachments if possible
        source_type = None
        if attachments:
            first_url = None
            for att in attachments:
                url = (
                    att.get("url")
                    or att.get("content_url")
                    or att.get("attachment_url")
                    or att.get("download_url")
                )
                if url:
                    first_url = url
                    source_type = self.detect_source_type(url)
                    break

            # Extract domain from URL if not in headers
            if first_url and not headers_dict.get("source_domain"):
                import re

                # This pattern covers all known source systems
                domain_patterns = [
                    (r"https://([^.]+)\.blob\.core\.windows\.net", "azure_blob"),
                    (
                        r"https://saasgeniestorage\.blob\.core\.windows\.net",
                        "azure_blob",
                    ),
                    (r"https://([^.]+)\.zendesk\.com", "zendesk"),
                    (r"https://([^.]+)\.freshdesk\.com", "freshdesk"),
                    (r"https://([^.]+)\.freshservice\.com", "freshservice"),
                    (r"https://([^.]+)\.service-now\.com", "servicenow"),
                    (r"https://([^.]+)\.atlassian\.net", "jira"),
                ]

                for pattern, detected_type in domain_patterns:
                    match = re.search(pattern, first_url)
                    if match:
                        source_domain = match.group(1)
                        if not source_type:
                            source_type = detected_type
                        logging.info(
                            f"Auto-detected source: {detected_type}, domain: {source_domain}"
                        )
                        break

        # Build auth config using source-specific logic
        auth_config = self._build_universal_auth_from_headers(headers_dict, source_type)

        if source_domain and auth_config:
            auth_config["source_domain"] = source_domain

        # Normalize each attachment
        for att in attachments:
            normalized = self._normalize_single_attachment(att, source_type)
            if normalized:
                normalized_attachments.append(normalized)

        return normalized_attachments, auth_config

    def _normalize_single_attachment(
        self, attachment: Dict, source_type: Optional[str]
    ) -> Dict:
        """
        Normalize a single attachment based on source type.
        Handles all source-specific field mappings.
        """
        normalized = attachment.copy()

        # Universal URL normalization
        if not normalized.get("url"):
            # Try various field names used by different sources
            url_fields = ["content_url", "attachment_url", "file_url", "download_url"]
            for field in url_fields:
                if attachment.get(field):
                    normalized["url"] = attachment[field]
                    break

        # Source-specific URL handling
        if source_type == "zendesk":
            # Zendesk-specific: prefer mapped_content_url if available
            if attachment.get("mapped_content_url"):
                normalized["url"] = attachment["mapped_content_url"]
                logging.debug(
                    f"Using Zendesk mapped_content_url for {attachment.get('file_name', 'attachment')}"
                )

        # Universal name normalization
        if not normalized.get("name"):
            name_fields = ["file_name", "filename", "fileName", "name", "title"]
            for field in name_fields:
                if attachment.get(field):
                    normalized["name"] = attachment[field]
                    break

            # Last resort - generate from URL or use default
            if not normalized.get("name"):
                if normalized.get("url"):
                    normalized["name"] = self._generate_filename_from_url(
                        normalized["url"]
                    )
                else:
                    normalized["name"] = "attachment"

        return normalized

    def _build_universal_auth_from_headers(
        self, headers_dict: Dict, detected_source_type: Optional[str] = None
    ) -> Dict:
        """
        Build authentication config from headers with source-specific handling.
        This centralizes ALL authentication logic in the attachment service.
        """
        auth_config = {}

        # Get source type
        source_type = (
            headers_dict.get("source_type", "").lower() or detected_source_type
        )

        # Check if domain indicates source type
        domain = headers_dict.get("domain", "")
        if not source_type:
            if "zendesk.com" in domain:
                source_type = "zendesk"
            elif "freshdesk.com" in domain:
                source_type = "freshdesk"
            elif "freshservice.com" in domain:
                source_type = "freshservice"
            elif "service-now.com" in domain:
                source_type = "servicenow"

        # Extract authentication based on source type
        if source_type == "zendesk":
            auth_config = self._extract_zendesk_auth(headers_dict)
        elif source_type == "freshdesk":
            auth_config = self._extract_freshdesk_auth(headers_dict)
        elif source_type == "freshservice":
            auth_config = self._extract_freshservice_auth(headers_dict)
        elif source_type == "servicenow":
            auth_config = self._extract_servicenow_auth(headers_dict)
        elif source_type == "jira":
            auth_config = self._extract_jira_auth(headers_dict)
        else:
            # Generic authentication extraction
            auth_config = self._extract_generic_auth(headers_dict)

        # Add source type to config
        if source_type:
            auth_config["source_type"] = source_type

        # Add source domain if available
        source_domain = headers_dict.get("source_domain")
        if source_domain:
            auth_config["source_domain"] = source_domain

        return auth_config

    def _extract_zendesk_auth(self, headers_dict: Dict) -> Dict:
        """Extract Zendesk-specific authentication"""
        auth_config = {}

        # Check for Zendesk's email/token format
        username = headers_dict.get("username", "")
        password = headers_dict.get("password", "")

        if username and password:
            if "/token" in username:
                # This is Zendesk API token authentication
                email = username.replace("/token", "")
                auth_config["email"] = email
                auth_config["api_token"] = password
                # Create the base64 encoded auth for Zendesk
                import base64

                credentials = base64.b64encode(
                    f"{username}:{password}".encode()
                ).decode()
                auth_config["api_key"] = credentials
            else:
                # Might be basic auth
                import base64

                credentials = base64.b64encode(
                    f"{username}:{password}".encode()
                ).decode()
                auth_config["api_key"] = credentials

        # Also check standard fields
        self._add_standard_auth_fields(headers_dict, auth_config)

        return auth_config

    def _extract_freshdesk_auth(self, headers_dict: Dict) -> Dict:
        """Extract Freshdesk-specific authentication"""
        auth_config = {}

        # Freshdesk uses API key authentication
        api_key = (
            headers_dict.get("source_api_key")
            or headers_dict.get("freshdesk_api_key")
            or headers_dict.get("fd_api_key")
        )

        if api_key:
            auth_config["api_key"] = api_key

        # Check username/password as fallback
        username = headers_dict.get("username", "")
        password = headers_dict.get("password", "")

        if username and password and not api_key:
            # Freshdesk expects email:password in base64
            import base64

            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            auth_config["api_key"] = credentials

        self._add_standard_auth_fields(headers_dict, auth_config)

        return auth_config

    def _extract_freshservice_auth(self, headers_dict: Dict) -> Dict:
        """Extract Freshservice-specific authentication"""
        auth_config = {}

        # Similar to Freshdesk
        api_key = (
            headers_dict.get("source_api_key")
            or headers_dict.get("freshservice_api_key")
            or headers_dict.get("fs_api_key")
        )

        if api_key:
            auth_config["api_key"] = api_key

        self._add_standard_auth_fields(headers_dict, auth_config)

        return auth_config

    def _extract_servicenow_auth(self, headers_dict: Dict) -> Dict:
        """Extract ServiceNow-specific authentication"""
        auth_config = {}

        # ServiceNow can use OAuth or basic auth
        oauth_token = headers_dict.get("source_oauth_token")
        if oauth_token:
            auth_config["oauth_token"] = oauth_token

        # Check for username/password
        username = headers_dict.get("username") or headers_dict.get("source_username")
        password = headers_dict.get("password") or headers_dict.get("source_password")

        logging.info(
            f"=== DEBUG: Found username: {username}, password: {'***' if password else 'None'}"
        )

        if username and password:
            auth_config["username"] = username
            auth_config["password"] = password

        self._add_standard_auth_fields(headers_dict, auth_config)

        return auth_config

    def _extract_jira_auth(self, headers_dict: Dict) -> Dict:
        """Extract Jira-specific authentication"""
        auth_config = {}

        # Jira uses email + API token
        email = headers_dict.get("source_email") or headers_dict.get("jira_email")
        api_token = headers_dict.get("source_api_token") or headers_dict.get(
            "jira_token"
        )

        if email and api_token:
            auth_config["email"] = email
            auth_config["api_token"] = api_token
            # Also create base64 for convenience
            import base64

            credentials = base64.b64encode(f"{email}:{api_token}".encode()).decode()
            auth_config["api_key"] = credentials

        self._add_standard_auth_fields(headers_dict, auth_config)

        return auth_config

    def _extract_generic_auth(self, headers_dict: Dict) -> Dict:
        """Extract generic authentication"""
        auth_config = {}
        self._add_standard_auth_fields(headers_dict, auth_config)
        return auth_config

    def _add_standard_auth_fields(self, headers_dict: Dict, auth_config: Dict):
        """Add standard authentication fields if not already present"""
        # Standard fields that might be present
        standard_fields = [
            "source_api_key",
            "sourceApiKey",
            "source_apikey",
            "source_oauth_token",
            "source_bearer_token",
            "source_access_token",
            "source_email",
            "sourceEmail",
            "source_api_token",
            "source_username",
            "source_password",
        ]

        field_mapping = {
            "source_api_key": "api_key",
            "sourceApiKey": "api_key",
            "source_apikey": "api_key",
            "source_oauth_token": "oauth_token",
            "source_bearer_token": "bearer_token",
            "source_access_token": "access_token",
            "source_email": "email",
            "sourceEmail": "email",
            "source_api_token": "api_token",
            "source_username": "username",
            "source_password": "password",
        }

        for field, target_field in field_mapping.items():
            value = headers_dict.get(field)
            if value and target_field not in auth_config:
                auth_config[target_field] = value

    def _generate_filename_from_url(self, url: str) -> str:
        """Generate a filename from URL"""
        try:
            from urllib.parse import urlparse, unquote

            parsed = urlparse(url)
            path_parts = parsed.path.split("/")

            # Look for actual filename in path
            for part in reversed(path_parts):
                if "." in part and len(part) > 1:
                    return unquote(part)

            # Check query params
            query_params = parse_qs(parsed.query)
            if "name" in query_params:
                return query_params["name"][0]
            if "filename" in query_params:
                return query_params["filename"][0]
        except:
            pass

        # Generate generic name
        return f"attachment_{int(time.time())}"

    # ===================================================================
    # SOURCE-SPECIFIC URL BUILDERS (ALL IN ONE PLACE)
    # ===================================================================

    def _build_zendesk_url(self, path: str, domain: str) -> str:
        """Build Zendesk URL from relative path"""
        return f"https://{domain}.zendesk.com{path}"

    def _build_freshdesk_url(self, path: str, domain: str) -> str:
        """Build Freshdesk URL from relative path"""
        return f"https://{domain}.freshdesk.com{path}"

    def _build_freshservice_url(self, path: str, domain: str) -> str:
        """Build Freshservice URL from relative path"""
        return f"https://{domain}.freshservice.com{path}"

    def _build_servicenow_url(self, path: str, domain: str) -> str:
        """Build ServiceNow URL from relative path"""
        return f"https://{domain}.service-now.com{path}"

    def _build_jira_url(self, path: str, domain: str) -> str:
        """Build Jira URL from relative path"""
        return f"https://{domain}.atlassian.net{path}"

    def _build_generic_url(self, path: str, domain: str) -> str:
        """Build generic URL from relative path"""
        return f"https://{domain}{path}"

    def get_statistics(self) -> Dict:
        """Get comprehensive statistics"""
        avg_time = (
            sum(self.stats["download_times"]) / len(self.stats["download_times"])
            if self.stats["download_times"]
            else 0
        )

        return {
            "total_downloads": self.stats["total_downloads"],
            "successful_downloads": self.stats["successful_downloads"],
            "failed_downloads": self.stats["failed_downloads"],
            "success_rate": self.stats["successful_downloads"]
            / max(self.stats["total_downloads"], 1)
            * 100,
            "total_bytes_downloaded": self.stats["total_bytes"],
            "average_download_time": avg_time,
            "total_download_time": sum(self.stats["download_times"]),
            "inline_images_processed": self.stats["inline_images_processed"],
            "inline_images_extracted": self.stats["inline_images_extracted"],
        }
