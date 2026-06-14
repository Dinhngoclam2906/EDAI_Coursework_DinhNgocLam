from pydantic import BaseModel
from typing import Optional


class CustomerRecord(BaseModel):
    customer_id: str
    full_name: str
    date_of_birth: str
    city: str
    country: str
    kyc_status: str       # verified | pending | rejected
    risk_tier: str        # low | medium | high
    signup_ts: str
    created_ts: str


class AccountRecord(BaseModel):
    account_id: str
    customer_id: str
    account_type: str     # savings | checking | credit
    balance: float
    currency: str
    status: str           # active | frozen | closed
    opened_ts: str
    created_ts: str


class CardRecord(BaseModel):
    card_id: str
    account_id: str
    card_type: str        # debit | credit
    masked_card_number: str
    expiry_date: str
    is_active: bool
    issued_ts: str
    created_ts: str


class MerchantRecord(BaseModel):
    merchant_id: str
    name: str
    mcc_code: str
    category: str
    city: str
    country: str
    is_active: bool
    created_ts: str


class TransactionRecord(BaseModel):
    transaction_id: str
    account_id: str
    card_id: str
    merchant_id: str
    amount: float
    currency: str
    channel: str          # online | pos | atm
    status: str           # approved | declined
    device_fingerprint: Optional[str]  # null before schema_change_date
    is_fraud: bool
    event_timestamp: str
    created_ts: str
    transaction_date: str


class FraudLabelRecord(BaseModel):
    label_id: str
    transaction_id: str
    is_fraud: bool
    fraud_type: str       # card_not_present | account_takeover | identity_theft | probing
    confirmed_ts: str
    created_ts: str
