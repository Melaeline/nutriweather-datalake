"""
Test script for Elasticsearch connection
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from include.scripts.elasticsearch_indexer import ElasticsearchIndexer

def test_connection():
    """Test Elasticsearch connection with localhost (since we're outside Docker)"""
    try:
        # Test with localhost for external access
        indexer = ElasticsearchIndexer(
            host="http://localhost:9200",  # Use localhost for external testing
            username="elastic",
            password="elastic"
        )
        
        print("✅ Elasticsearch connection successful!")
        
        # Test index setup
        print("\nTesting index setup...")
        setup_results = indexer.setup_indices()
        print(f"Setup results: {setup_results}")
        
        # Test with some sample data
        print("\nTesting document indexing...")
        sample_docs = [
            {
                "food_name": "Apple",
                "calories": 95,
                "protein": 0.5,
                "fat": 0.3,
                "carbohydrates": 25,
                "category": "fruit"
            },
            {
                "food_name": "Banana", 
                "calories": 105,
                "protein": 1.3,
                "fat": 0.4,
                "carbohydrates": 27,
                "category": "fruit"
            }
        ]
        
        result = indexer.index_documents("test-nutrition", sample_docs)
        print(f"Indexing result: {result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()
