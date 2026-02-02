from dagster import AssetExecutionContext
from pydantic import BaseModel, StrictStr, StrictInt, StrictBool, ValidationError, Field
from typing import List, Any, Dict, Union


class GuidObject(BaseModel):
    rendered: StrictStr

class TitleObject(BaseModel):
    rendered: StrictStr

class ContentObject(BaseModel):
    rendered: StrictStr
    protected: StrictBool

class ExcerptObject(BaseModel):
    rendered: StrictStr
    protected: StrictBool

class CaptionObject(BaseModel):
    rendered: StrictStr

class DescriptionObject(BaseModel):
    rendered: StrictStr

class MediaDetailsObject(BaseModel):
    filesize: StrictInt
    sizes: Dict[str, Any]
    
    # class Config:
    #     extra = "allow"  
    
class ExpectedPagesSchema(BaseModel):
    """Expected JSON schema from the WordPress Pages API with strict type enforcement."""
    id: StrictInt
    date: StrictStr
    date_gmt: StrictStr
    guid: GuidObject
    modified: StrictStr
    modified_gmt: StrictStr
    slug: StrictStr
    status: StrictStr
    type: StrictStr
    link: StrictStr
    title: TitleObject
    content: ContentObject
    excerpt: ExcerptObject
    author: StrictInt
    featured_media: StrictInt
    parent: StrictInt
    menu_order: StrictInt
    comment_status: StrictStr
    ping_status: StrictStr
    template: StrictStr
    meta: Union[Dict[str, Any], list] = Field(default_factory=dict)
    links: Dict[str, Any] = Field(alias="_links", default_factory=dict)

    class Config:
        extra = "forbid" 
        populate_by_name = True

class ExpectedMediaSchema(BaseModel):
    """Expected JSON schema from the WordPress Media API with strict type enforcement."""
    id: StrictInt
    date: StrictStr
    date_gmt: StrictStr
    guid: GuidObject
    modified: StrictStr
    modified_gmt: StrictStr
    slug: StrictStr
    status: StrictStr
    type: StrictStr
    link: StrictStr
    title: TitleObject
    author: StrictInt
    comment_status: StrictStr
    ping_status: StrictStr
    template: StrictStr
    meta: Union[Dict[str, Any], list] = Field(default_factory=dict)
    description: DescriptionObject
    caption: CaptionObject
    alt_text: StrictStr
    media_type: StrictStr
    mime_type: StrictStr
    media_details: Union[MediaDetailsObject, Dict[str, Any]] = Field(default_factory=dict)
    post: Union[StrictInt, None] = None
    source_url: StrictStr
    links: Dict[str, Any] = Field(alias="_links", default_factory=dict)

    class Config:
        extra = "forbid"
        populate_by_name = True

# Mapping of endpoint keys to their corresponding schema classes
expected_endpoint_schemas = {
    "pages": ExpectedPagesSchema,
    "media": ExpectedMediaSchema,
}

class Violation(BaseModel):
    """Schema violation notification storage."""
    kind: str
    field: str
    message: str

class SchemaViolationError(Exception):
    """Custom exception to represent schema violations."""
    def __init__(self, id, violation: Violation, message: str = None):
        self.id = id
        self.violation = violation
        self.message = message
        super().__init__(self._format())

    def _format(self):
        lines = [f"Schema violation for id={self.id}:"]
        for d in self.violation:
            lines.append(f"- [{d.kind}] {d.field}: {d.message}")
        return "\n".join(lines)
    
def validate_single_response(data: dict, endpoint_key: str, context: AssetExecutionContext) -> Union[ExpectedPagesSchema, ExpectedMediaSchema]:
    """Validate a single WordPress API response against the expected schema.
    
    Args:
        data (dict): Dictionary containing WordPress data.
        endpoint_key (str): The endpoint key (e.g., 'pages', 'media') to determine schema.
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.

    Returns:
        Union[ExpectedPagesSchema, ExpectedMediaSchema]: Validated Pydantic model instance.

    Raises:
        SchemaViolationError: Custom exception containing details of schema violations.
        ValueError: If no schema is defined for the given endpoint.
    """
    violations = []
    response_id = str(data.get('id', '<missing_id>'))
    
    # Get the appropriate schema class for this endpoint
    schema_class = expected_endpoint_schemas.get(endpoint_key)
    if not schema_class:
        raise ValueError(f"No schema defined for endpoint: {endpoint_key}. Available endpoints: {list(expected_endpoint_schemas.keys())}")

    try:
        return schema_class(**data)
    except ValidationError as e:
        for error in e.errors():
            field_path = ".".join(str(loc) for loc in error['loc'])
            violations.append(Violation(
                kind=error['type'],
                field=field_path,
                message=error['msg']
            ))
        raise SchemaViolationError(
            id=response_id, 
            violation=violations, 
            message=f"Schema validation failed for endpoint '{endpoint_key}'"
        )

def validate_batch_responses(data: List[dict], endpoint_key: str, context: AssetExecutionContext) -> List[Union[ExpectedPagesSchema, ExpectedMediaSchema]]:
    """Validate schema of all fetched WordPress API responses.
    
    Args:
        data (List[dict]): List of dictionaries containing WordPress data.
        endpoint_key (str): The endpoint key (e.g., 'pages', 'media') to determine schema.
        context (AssetExecutionContext): Dagster AssetExecutionContext for logging.
    
    Returns:
        List[Union[ExpectedPagesSchema, ExpectedMediaSchema]]: List of validated Pydantic models.
    
    Raises:
        SchemaViolationError: Custom exception containing details of schema violations.
    """
    validated_records = []
    for item in data:
        validated_records.append(validate_single_response(item, endpoint_key, context))
    return validated_records

 