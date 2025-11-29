# SolarWinds to Freshservice Field Mapping Analysis

## Executive Summary
This document provides a comprehensive analysis of field mappings between SolarWinds Service Desk (Samanage) and Freshservice Change Management.

## Current Implementation Status

### ✅ Successfully Mapped Fields

#### Core Change Fields
1. **subject** ← `name` (SolarWinds)
2. **description** ← `description` (SolarWinds)
3. **planned_start_date** ← `scheduled_for` (SolarWinds)
4. **planned_end_date** ← `end_time` (SolarWinds)

#### Status & Priority Fields (Value Mapped)
5. **status** ← `state` (Value mapping configured)
   - Approval Requested → 1 (Open)
   - Planning → 1 (Open)
   - Awaiting Approval → 2 (Planning)
   - Pending Release → 2 (Planning)
   - Pending Review → 3 (Pending Release)
   - Closed → 4 (Closed)
   - Cancelled → 5 (Cancelled)
   - Rejected → 6 (Rejected)

6. **priority** ← `priority` (Value mapping configured)
   - Critical → 4
   - High → 3
   - Medium → 2
   - Low → 1

7. **risk** ← `risk` (Value mapping configured)
   - Minor → 1
   - Standard → 2
   - Major → 3
   - Emergency → 4

8. **impact** ← `impact` (Value mapping configured)
   - Low → 1
   - Medium → 2
   - High → 3

9. **change_type** ← `change_type` (Value mapping configured)
   - Minor → 1
   - Standard → 2
   - Major → 3
   - Emergency → 4

#### Assignment Fields
10. **requester_email** ← `requester.email` (SolarWinds)
11. **agent_id** ← `assignee.email` (Value mapped to Freshservice agent ID)
12. **group_id** ← Static value: 24000267577

#### Planning Fields (Special Handling Required)
13. **change_plan** ← `change_plan` (SolarWinds) - Multipart form-data
14. **rollback_plan** ← `rollback_plan` (SolarWinds) - Multipart form-data
15. **test_plan** ← `test_plan` (SolarWinds) - Multipart form-data

**Note**: These three planning fields map to Freshservice UI fields:
- change_plan → "Reason for Change"
- rollback_plan → "Backout Plan" / "Rollout Plan"
- test_plan → "Impact"

### ✅ Newly Added Field Mappings

16. **category** ← `category.name` (SolarWinds)
17. **sub_category** ← `subcategory.name` (SolarWinds)
18. **department_id** ← `department.id` (SolarWinds)
19. **approval_status** ← `approval_status` (SolarWinds, read-only)

## Freshservice API Field Reference

### Required Fields (Mandatory)
- ✅ `subject` - String
- ✅ `requester_id` - Number (resolved from requester_email)
- ✅ `planned_start_date` - DateTime (ISO8601)
- ✅ `planned_end_date` - DateTime (ISO8601)
- ✅ `status` - Number (1-6)
- ✅ `priority` - Number (1-4)
- ✅ `impact` - Number (1-3)
- ✅ `risk` - Number (1-4)
- ✅ `change_type` - Number (1-4)

### Optional Fields (Commonly Used)
- ✅ `description` - String (HTML)
- ✅ `agent_id` - Number
- ✅ `group_id` - Number
- ✅ `category` - String
- ✅ `sub_category` - String
- ✅ `department_id` - Number
- ⏳ `custom_fields` - Hash (for additional custom fields)
- ⏳ `assets` - Array (associated assets/CIs)

### Special Fields (Multipart Form-Data Required)
- ✅ `planning_fields[change_plan]` - String
- ✅ `planning_fields[rollback_plan]` - String  
- ✅ `planning_fields[test_plan]` - String

### Read-Only Fields (System Managed)
- `id` - Change ID
- `created_at` - Creation timestamp
- `updated_at` - Last update timestamp
- `approval_status` - Approval workflow status
- `workspace_id` - Workspace/client ID

## SolarWinds API Field Reference

### Available Change Fields
Based on Samanage API documentation:
- `id` - Change ID
- `name` - Change title
- `description` - Change description (HTML)
- `state` - Change state/status
- `priority` - Priority level
- `impact` - Impact assessment
- `risk` - Risk level
- `change_type` - Type of change
- `scheduled_for` - Scheduled start date/time
- `end_time` - Scheduled end date/time
- `requester` - Requester object (contains email, name, etc.)
- `assignee` - Assigned agent object
- `category` - Category object
- `subcategory` - Subcategory object
- `department` - Department object
- `change_plan` - Implementation plan
- `rollback_plan` - Rollback procedure
- `test_plan` - Testing plan
- `approval_status` - Approval workflow status

## Technical Implementation Notes

### Planning Fields Handling
The planning_fields in Freshservice **require multipart/form-data** format:

```python
# Correct format
form_data = [
    ('planning_fields[change_plan]', 'Implementation steps...'),
    ('planning_fields[rollback_plan]', 'Rollback procedure...'),
    ('planning_fields[test_plan]', 'Testing strategy...')
]
```

**Implementation Location**: `src/connector/freshservicev2.py` (lines 1248-1280)

### Email to ID Resolution
The `requester_email` field is automatically resolved to `requester_id` by `FreshworksHelper`:

```python
# Located in freshservicev2.py lines 1093-1102
final_data = helper.resolve_requester_id(change_data, headers)
```

### Value Mapping
Status, priority, risk, impact, and change_type use value mapping to convert SolarWinds text values to Freshservice numeric codes.

## Potential Enhancements

### Fields Available But Not Yet Mapped
1. **custom_fields** - For SolarWinds custom fields
2. **assets** - Associated configuration items
3. **maintenance_window** - Maintenance window association
4. **item_category** - Item categorization
5. **cc_emails** - CC email addresses

### Dependent Resources (Currently Skipped)
1. **tasks** - Change tasks (configured but `skip: true`)
2. **notes** - Change notes/comments (configured but `skip: true`)
3. **attachments** - File attachments
4. **time_entries** - Time tracking
5. **approvals** - Approval workflow

## Testing Recommendations

### Test Cases to Execute
1. ✅ Create change without planning_fields - **PASSED (201)**
2. ⏳ Create change with planning_fields - **READY TO TEST**
3. ⏳ Verify planning fields populate in UI
4. ⏳ Test all value mappings (status, priority, risk, impact, change_type)
5. ⏳ Test category and sub_category mapping
6. ⏳ Enable and test tasks migration
7. ⏳ Enable and test notes migration

### Verification Steps
1. Create test change in SolarWinds with all fields populated
2. Run migration
3. Verify in Freshservice UI:
   - "Reason for Change" contains change_plan
   - "Rollout Plan" contains rollback_plan  
   - "Impact" contains test_plan
   - Category and sub-category are correct
   - All other mapped fields are accurate

## API Documentation References

### Freshservice
- **Create Change API**: https://api.freshservice.com/#create_change
- **Change Fields**: https://api.freshservice.com/#list_all_change_fields
- **Planning Fields**: Require multipart/form-data format

### SolarWinds (Samanage)
- **Get Change by ID**: https://apidoc.samanage.com/#tag/Change/operation/getChangeById
- **API Base URL**: `api.samanage.com`
- **Authentication**: JWT Bearer token

## Configuration File Location
`test/data/migration_configs/solarwinds_changes_to_freshservice.json`

## Key Code Files
- `src/connector/freshservicev2.py` - Freshservice API connector
- `src/connector/solarwinds.py` - SolarWinds API connector
- `src/transform.py` - Request routing and handler mappings

## Known Issues & Solutions

### Issue 1: Planning Fields Not Populating
**Cause**: Freshservice API rejects planning_fields in JSON format  
**Solution**: Use multipart/form-data format (implemented in lines 1248-1280)

### Issue 2: requester_email KeyError
**Cause**: Config used 'email' instead of 'requester_email'  
**Solution**: Updated JSON config to use correct attribute name

### Issue 3: 500 Error with Planning Fields
**Cause**: Attempted to send planning_fields as nested JSON object  
**Solution**: Flatten to form-data with `planning_fields[field_name]` format

## Next Steps
1. Test multipart planning_fields implementation
2. Verify all planning fields populate correctly
3. Consider enabling tasks and notes migration
4. Add custom_fields support if needed
5. Add assets/CI association if required

---
**Document Version**: 1.0  
**Last Updated**: 2025-01-19  
**Author**: GitHub Copilot
