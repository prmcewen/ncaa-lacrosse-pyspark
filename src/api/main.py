from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from src.api.dependencies import graphql_context
from src.api.graphql.schema import schema
from src.api.rest.routes import router as rest_router

app = FastAPI(
    title="NCAA Lacrosse Data Platform API",
    description="NCAA men's lacrosse play-by-play analytics through REST and GraphQL",
    version="0.1.0"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Strawberry GraphQL Router
graphql_app = GraphQLRouter(schema, context_getter=graphql_context)
app.include_router(graphql_app, prefix="/graphql", tags=["GraphQL Endpoint"])

# Mount REST Routes
app.include_router(rest_router)


@app.get("/")
def root():
    return {
        "message": "NCAA Men's Lacrosse Analytics API is active",
        "rest_docs": "/docs",
        "graphql_playground": "/graphql"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
