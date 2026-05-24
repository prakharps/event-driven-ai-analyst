import os
import logging
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings
from config.settings import AppSettings

logger = logging.getLogger("AIProcessingEngine.VectorMemory")

class VectorMemoryStore:
    def __init__(self):
        logger.info("Initializing ChromaDB persistent vector repository...")
        
        # Initialize standard OpenAI text embedding engine
        self.embeddings = OpenAIEmbeddings()
        
        # Bind to persistent local directory
        self.vector_store = Chroma(
            collection_name="historical_resolutions",
            embedding_function=self.embeddings,
            persist_directory=AppSettings.CHROMA_PERSIST_DIR
        )
        
        # Seed the store with initial historical contexts if it's completely empty
        self._seed_historical_knowledge_base()

    def find_similar_cases(self, query: str, limit: int = 2) -> str:
        """Performs a semantic similarity search across our embedding collection."""
        try:
            results = self.vector_store.similarity_search(query, k=limit)
            if not results:
                return "No matching historical reference resolutions found."
                
            formatted_references = []
            for idx, doc in enumerate(results, 1):
                formatted_references.append(f"--- Reference Case #{idx} ---\n{doc.page_content}")
                
            return "\n\n".join(formatted_references)
        except Exception as e:
            logger.error(f"Semantic memory retrieval failure: {e}")
            return "Failed to extract historical contextual memories due to an internal error."

    def _seed_historical_knowledge_base(self):
        """Pre-populates the vector engine with standardized golden engineering resolutions."""
        # Simple count check to avoid redundant seeding on container restarts
        existing_docs = self.vector_store.get()
        if existing_docs and len(existing_docs.get("ids", [])) > 0:
            logger.info("Vector database contains existing items. Skipping seeding phase.")
            return

        logger.info("Vector database empty. Seeding golden resolution historical files...")
        
        mock_cases = [
            "ISSUE: 504 Gateway Timeout errors occurring during peak database write operations.\n"
            "RESOLUTION: Scale up the write capacity units on the transactional store and recycle the internal ingress network load balancers. Priority: CRITICAL.",
            
            "ISSUE: Free tier users demanding higher data exports limits, complaining about UI lockouts.\n"
            "RESOLUTION: Advise the customer regarding corporate data limitations tied to compliance restrictions on non-paid infrastructure tiers. Priority: HIGH.",
            
            "ISSUE: Forgotten password link fails to send a verification email token to the user.\n"
            "RESOLUTION: The transactional email route was congested. Cleared queue and re-routed through backup relay nodes. Priority: MEDIUM."
        ]
        
        self.vector_store.add_texts(texts=mock_cases)
        self.vector_store.persist()
        logger.info("Successfully seeded historical vector records.")

    def save_new_resolution(self, issue_text: str, ai_resolution_data: dict):
        """
        Converts a newly completed ticket and its AI resolution into a 
        standardized semantic document and commits it directly to the vector store index.
        """
        try:
            # Standardize the text format so it matches our search schema perfectly
            document_content = (
                f"ISSUE CATEGORY: {ai_resolution_data.get('sentiment_analysis', 'General Inquiry')}\n"
                f"USER REPORT: {issue_text}\n"
                f"RESOLUTION APPROACH: {ai_resolution_data.get('suggested_action', '')} "
                f"Automated Response Used: {ai_resolution_data.get('automated_response', '')}\n"
                f"Priority: {ai_resolution_data.get('priority_level', 'MEDIUM')}."
            )
            
            # Embed and save to disk
            self.vector_store.add_texts(texts=[document_content])
            
            # Explicitly persist data to the local directory
            self.vector_store.persist()
            logger.info("Successfully committed new resolution instance to long-term vector memory.")
        except Exception as e:
            logger.error(f"Failed to commit runtime transaction to vector database: {e}")