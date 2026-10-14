from pydantic import BaseModel, Field, model_validator


class CreatePayment(BaseModel):
    amount_minor: int = Field(gt=0, description="Amount in minor units (cents).")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    source_account: str = Field(min_length=1, max_length=64)
    destination_account: str = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def accounts_differ(self) -> "CreatePayment":
        if self.source_account == self.destination_account:
            raise ValueError("source_account and destination_account must differ")
        return self
