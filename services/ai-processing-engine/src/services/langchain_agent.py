from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from config.settings import AppSettings
from models.schemas import TicketResolution

class LangChainAgent:
    def __init__(self):
        # Initialize the LLM with low temperature for deterministic parsing
        self.llm = ChatOpenAI(
            model=AppSettings.OPENAI_MODEL, 
            temperature=AppSettings.LLM_TEMPERATURE
        )
        self.structured_llm = self.llm.with_structured_output(TicketResolution)
        self.prompt_template = self._build_prompt_template()

    def _build_prompt_template(self):
        return ChatPromptTemplate.from_messages([
            ("system", """You are a Principal Customer Experience Architect and AI Systems Automation Analyst. 
            Your job is to analyze incoming system tickets, extract root operational issues, and synthesize them against enterprise account contexts.

            CRITICAL METRIC RULES:
            1. If a customer's `churn_risk_score` is greater than 0.75 AND they are an Enterprise tier, automatically upgrade priority to CRITICAL.
            2. If they are a Free tier, their priority cap is HIGH unless it's a structural failure.
            
            "HISTORICAL CORPORATE MEMORY (Use these past resolutions for guidelines):\n"
                "{historical_memory}\n\n"
                        

            Be concise, accurate, and completely professional."""),
                        ("human", """
            [RAW INCOMING TICKET]
            Ticket ID: {ticket_id}
            Issue Description: {issue_text}

            [ENRICHED ACCOUNT CONTEXT]
            Company Name: {company_name}
            Subscription Tier: {subscription_tier}
            Monthly Account Spend: ${monthly_spend}
            Churn Risk Score: {churn_risk_score}
            Assigned Account Manager: {account_manager}

            Analyze the data above and generate the required structured resolution schema.
            """)
        ])

    def resolve_ticket(self, ticket_id: str, issue_text: str, context: dict, historical_memory: str) -> TicketResolution:
        formatted_prompt = self.prompt_template.format_messages(
            ticket_id=ticket_id,
            issue_text=issue_text,
            company_name=context['company_name'],
            subscription_tier=context['subscription_tier'],
            monthly_spend=float(context['monthly_spend']),
            churn_risk_score=context['churn_risk_score'],
            account_manager=context['account_manager'],
            historical_memory=historical_memory,
        )
        return self.structured_llm.invoke(formatted_prompt)