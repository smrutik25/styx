from pydantic import BaseModel, Field, ValidationError, TypeAdapter
from typing import Union, List, Annotated, Literal
from .exceptions import InvalidAnalyticalSchema


class ColumnBaseSchema(BaseModel):
    type: Literal["base"] = "base"
    column_name: str
    data_type: str
    datetime_format: Annotated[str, Field(default=None)]
    nullable: Annotated[bool, Field(default=False)]
    primary_key: Annotated[bool, Field(default=False)]
    reference: Annotated[str, Field(default=None)]


class NestedColumnSchema(ColumnBaseSchema):
    nested_source_column: str


class NestedTableSchema(ColumnBaseSchema):
    type: Literal["nested"] = "nested"
    unnest: bool
    unnest_table_name: Annotated[str, Field(default=None)]
    nested_column_mapping: List[NestedColumnSchema]
    custom_unnest_logic: Annotated[str, Field(default=None)]


ColumnSchema = Annotated[Union[ColumnBaseSchema, NestedTableSchema], Field(discriminator="type")]
ColumnAdapter = TypeAdapter(ColumnSchema)


class QueryEngineSchema:
    def __init__(self, schema: list[dict]):
        self._column_schemas: List[ColumnSchema] = []
        for column in schema:
            processed_data = self._preprocess_data(column)
            self._parse_columns(processed_data)

    @property
    def column_schemas(self) -> List[ColumnSchema]:
        return self._column_schemas

    @staticmethod
    def _preprocess_data(column: dict) -> dict:
        if column.get("unnest"):
            column["type"] = "nested"
            for nested_col in column.get("nested_column_mapping", []):
                nested_col["type"] = "base"
        else:
            column["type"] = "base"
        return column

    def _parse_columns(self, column: dict) -> None:
        try:
            column = ColumnAdapter.validate_python(column)
            self._column_schemas.append(column)
        except ValidationError as e:
            raise InvalidAnalyticalSchema(f"Validation failed for column: {column['column_name']}\n{e}")
