import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.base import Base
from models.campaign_entity import CampaignEntity
from models.card_entity import CardEntity
from models.rule_entity import RuleEntity
from schema.campaign_schema import CampaignSchema

# 1️⃣ Connect to PostgreSQL
engine = create_engine("postgresql://postgres:password@localhost:5432/mydb")
Session = sessionmaker(bind=engine)
session = Session()

# 2️⃣ Create Tables
Base.metadata.create_all(engine)

# 3️⃣ Load JSON
with open("sample.json", "r") as f:
    data = json.load(f)

campaign_schema = CampaignSchema(**data)

# 4️⃣ Convert Schema → Entities
campaign = CampaignEntity(
    id=campaign_schema.id,
    slug=campaign_schema.slug,
    title=campaign_schema.title,
    status=campaign_schema.status,
    type=campaign_schema.type
)

for card_data in campaign_schema.cards:
    card = CardEntity(
        content=card_data.content,
        design=card_data.design,
        campaign=campaign
    )

    # Add customer rules
    for rule in card_data.customerRules:
        r = RuleEntity(ruleType="customer", value=rule.value, card_id=card.id)
        card.customerRules.append(r)

    # Add business rules
    for rule in card_data.businessRules:
        r = RuleEntity(ruleType="business", value=rule.value, card_id=card.id)
        card.businessRules.append(r)

# 5️⃣ Save (Cascade)
session.add(campaign)
session.commit()
session.close()

print("ETL completed successfully!")
