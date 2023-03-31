import uvicorn
from fastapi import FastAPI
from mangum import Mangum
import os

from model import CollectionListPOSTRequest, AugmentedCollectionListPOSTResponse
from preprocessing_service import preprocess_collections


app = FastAPI(
    root_path=os.environ.get("API_STAGE", "dev"),  # API_STAGE in ('dev', 'test', 'prod')
    docs_url='/docs',
    title='Collections preprocessing',
    description='This service provides preprocessing of collections with POST at /collections endpoint.',
    version='0.0.1'
)


@app.get("/ping", name="Healthcheck")
async def healthcheck():
    return {"success": "pong!"}


@app.post("/collections/")
async def get_augmented_collections(collections_request: CollectionListPOSTRequest):
    augmented_collection_list = preprocess_collections(collections_request.data)
    return AugmentedCollectionListPOSTResponse(data=augmented_collection_list)


handler = Mangum(app)

if __name__ == '__main__':
    uvicorn.run(app=app, port=9100)
