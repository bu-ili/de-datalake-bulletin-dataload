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
    
class ExpectedJSONSchema(BaseModel):
    """Definition of the expected JSON schema from the WordPress Pages API, with strict type enforcement."""
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

class Violation(BaseModel):
    """Definition to store notification of schema violation."""
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
    
def validate_single_response(data: dict, context: AssetExecutionContext) -> ExpectedJSONSchema:
    """
    Validate a single WordPress API response against the expected schema defined in ExpectedJSONSchema Pydantic class.
    
    Arguments:
        data: Dictionary containing WordPress page data
        context: Dagster AssetExecutionContext for logging purposes

    Returns:
        ExpectedJSONSchema: Validated Pydantic model instance

    Raises:
        SchemaViolationError: Custom exception containing details of schema violations to provide troubleshooting information in Dagster UI
    """
    violations = []
    response_id = str(data.get('id', '<missing_id>'))

    try:
        return ExpectedJSONSchema(**data)
    except ValidationError as e:
        for error in e.errors():
            field_path = ".".join(str(loc) for loc in error['loc'])
            violations.append(Violation(
                kind=error['type'],
                field=field_path,
                message=error['msg']
            ))
        raise SchemaViolationError(id=response_id, violation=violations, message="Schema validation failed")

def validate_batch_responses(data: List[dict], context: AssetExecutionContext) -> List[ExpectedJSONSchema]:
    """
    Validate schema of all fetched WordPress API responses.
    
    Arguments:
        data: List of dictionaries containing WordPress pages data
        context: Dagster AssetExecutionContext for logging purposes
    
    Returns:
        List[ExpectedJSONSchema]: List of validated Pydantic model instances
    
    Raises:
        SchemaViolationError: Custom exception containing details of schema violations to provide troubleshooting information in Dagster UI
    """
    validated_records = []
    for item in data:
        validated_records.append(validate_single_response(item, context))
    return validated_records

 