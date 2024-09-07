from bson import ObjectId
from pydantic import BaseModel, Field


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


class CPEBase(BaseModel):
    # Parsed CPE fields based on your requirement
    cpe_version: str
    part: str
    vendor: str
    product: str
    version: str
    update: str
    edition: str
    language: str
    sw_edition: str
    target_sw: str
    target_hw: str
    other: str
    cpe_id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class CPECreate(CPEBase):
    pass


class CPEResponse(CPEBase):
    pass
