from pydantic import BaseModel

class ResultRequest(BaseModel):
    name: str
    direccion: str
    ciudad: int