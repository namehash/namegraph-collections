# Collections preprocessing

Fastapi service providing preprocessing of collections.
- `POST /collections` - endpoint for preprocessing collections
- `wordninja` used for tokenization
- `mangum` used for wrapping FastAPI app and triggering it as lambda handler
- When deploying this lambda with AWS API Gateway set `API_STAGE` 
environment variable ('dev'/'test'/'prod') in Lambda runtime, it will be used as `root_path`; it should have the same value as`api_stage` in the context of AWS API Gateway
  - https://docs.aws.amazon.com/lambda/latest/dg/services-apigateway.html#apigateway-proxy
  - https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-stages.html
  - https://rafrasenberg.com/fastapi-lambda/#Deploying_FastAPI_lambda_with_Terraform

## Example request:
```
{
    "data": [
                {
                    "collection_name": "marvel heroes",
                    "keywords": ["marvel", "superhero"],
                    "names": [
                        "hulk", "CaptainAmerica", "deadpool", "doctorstrange"
                    ],
                    "description": "marvel heroes doing epic things"
                }
    ]
}
```