from pydantic import BaseModel, Field

class TicketResolution(BaseModel):
    priority_level: str = Field(
        description="Must be one of: CRITICAL, HIGH, MEDIUM, LOW. Determine this by combining the issue severity with the customer subscription tier."
    )
    sentiment_analysis: str = Field(
        description="Brief assessment of the customer's emotional state, e.g., Frustrated, Neutral, Calm."
    )
    suggested_action: str = Field(
        description="The precise mechanical next step for engineering or account managers based on the problem context."
    )
    automated_response: str = Field(
        description="A highly customized, professional, empathetic direct email response tailored specifically to their subscription tier."
    )