import tracemalloc

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from src.transform import Transformer

from src.utils.logger import setup_logger
from src.utils.migration_config_parser import read_migration_config
from src.utils.config import CONFIG
from src.utils.redis_client import RedisClient

app = FastAPI()
logger = setup_logger()
tracemalloc.start()
redis_client = RedisClient()

class MigrationRequest(BaseModel):
    migration_id: str

def main(logger, migration_id, migration_configs):
    logger.info(f'''
    Starting the migration transformation for the plan with name: {migration_configs["name"]} with id: {migration_id} started by {migration_configs["executedBy"]}.
    Parallel processing at root levels {"enabled" if CONFIG["PARALLEL_PROCESS_ROOT_ENABLED"] else "disabled"}, nested levels {"enabled" if CONFIG["PARALLEL_PROCESS_CHILDREN_ENABLED"] else "disabled"}
    ''', extra={'migration_id': migration_id})
    
    # before = tracemalloc.take_snapshot()

    transformer = Transformer(
        logger=logger,
        migration_id=migration_id,
        source_name=migration_configs['sourceModuleName'],
        target_name=migration_configs['targetModuleName'],
        configs=migration_configs,
        redis_client=redis_client
    )
    transformer.run()
    # Delete the termination key to avoid future checks
    termination_status_key = f'mg_{migration_id}_should_terminate'

    redis_client.delete(termination_status_key)

    # after = tracemalloc.take_snapshot()
    # stats = after.compare_to(before, 'lineno')
    # # Current and peak memory usage
    # current, peak = tracemalloc.get_traced_memory()
    # logger.info(f'Current memory usage: {current / 1024 / 1024:.1f} MB. Peak usage: {peak / 1024 / 1024:.1f} MB', extra={
    #     'migration_id': migration_id
    # })

    # logger.info('[Memory Diff] Top memory differences after transformer.run()', extra={'migration_id': migration_id})
    # for stat in stats[:10]:
    #     logger.info(f'[Memory Diff] {stat}')


@app.post('/transform')
async def trigger_transformation(request: MigrationRequest, background_tasks: BackgroundTasks):
    if not request.migration_id.strip():
        raise HTTPException(status_code=400, detail='Invalid / missing migration_id')
    
    migration_id = request.migration_id
    
    logger.info('Reading the migration configs json',
                    extra={'migration_id': migration_id})

    migration_configs = read_migration_config(logger, migration_id)

    if not migration_configs:
        raise HTTPException(status_code=400, detail='Error fetching migration configs')

    background_tasks.add_task(main, logger, request.migration_id, migration_configs)

    return {
        'message': f'Successfully started transformation for the migration with id : {request.migration_id}',
    }
