import sys
import os
import math
import unittest

# Add spark path to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

import data_profile_store
from ai_rule_advisor import AIRuleAdvisor

class TestProfileAndML(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a local Spark Session for testing
        cls.spark = SparkSession.builder \
            .appName("TestProfileAndML") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_compute_current_profile(self):
        # Create a dummy DataFrame
        data = [
            (20.0, "A"), (30.0, "B"), (40.0, None), (50.0, "A"), (None, "B")
        ]
        schema = StructType([
            StructField("age", DoubleType(), True),
            StructField("category", StringType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)

        profile = data_profile_store.compute_current_profile(df, ["age"], ["age", "category"])
        
        # Test Null profiles
        self.assertIn("age", profile)
        self.assertIn("category", profile)
        self.assertEqual(profile["age"]["null_profile"]["null_count"], 1)
        self.assertEqual(profile["age"]["null_profile"]["current_null_rate"], 0.2)
        self.assertEqual(profile["category"]["null_profile"]["null_count"], 1)
        self.assertEqual(profile["category"]["null_profile"]["current_null_rate"], 0.2)

        # Test Distribution profiles
        age_dist = profile["age"]["distribution"]
        self.assertEqual(age_dist["min"], 20.0)
        self.assertEqual(age_dist["max"], 50.0)
        self.assertEqual(age_dist["mean"], 35.0)  # (20+30+40+50)/4
        self.assertEqual(age_dist["non_null_count"], 4)

        # Test Cardinality
        self.assertEqual(profile["age"]["cardinality"]["distinct_count"], 5)  # including null

    def test_update_profiles_ema(self):
        stored = {
            "age": {
                "distribution": {
                    "mean": 30.0, "stddev": 5.0, "median": 30.0,
                    "p5": 20.0, "p25": 25.0, "p75": 35.0, "p95": 40.0,
                    "skewness": 0.0, "kurtosis": 0.0, "min": 10.0, "max": 50.0
                },
                "null_profile": {
                    "null_rate_ema": 0.1,
                    "null_rate_history": [0.1]
                },
                "profile_version": 1
            }
        }
        current = {
            "age": {
                "distribution": {
                    "mean": 40.0, "stddev": 10.0, "median": 40.0,
                    "p5": 20.0, "p25": 30.0, "p75": 50.0, "p95": 60.0,
                    "skewness": 0.1, "kurtosis": 0.2, "min": 5.0, "max": 65.0,
                    "non_null_count": 100
                },
                "null_profile": {
                    "current_null_rate": 0.2
                },
                "cardinality": {
                    "distinct_count": 50
                }
            }
        }

        updated = data_profile_store.update_profiles_ema(stored, current)
        age_up = updated["age"]
        
        # Alpha is 0.3. New mean = 0.3 * 40 + 0.7 * 30 = 33.0
        self.assertAlmostEqual(age_up["distribution"]["mean"], 33.0)
        # New null rate EMA = 0.3 * 0.2 + 0.7 * 0.1 = 0.13
        self.assertAlmostEqual(age_up["null_profile"]["null_rate_ema"], 0.13)
        self.assertEqual(age_up["profile_version"], 2)
        # min/max should be outer bounds
        self.assertEqual(age_up["distribution"]["min"], 5.0)
        self.assertEqual(age_up["distribution"]["max"], 65.0)

    def test_compute_psi_and_drift(self):
        # Create normal and shifted datasets
        normal_data = [(float(i),) for i in range(100)]
        shifted_data = [(float(i + 20),) for i in range(100)]
        schema = StructType([StructField("val", DoubleType(), True)])
        
        df_normal = self.spark.createDataFrame(normal_data, schema)
        df_shifted = self.spark.createDataFrame(shifted_data, schema)

        # Baseline boundaries (quantiles)
        boundaries = [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]

        psi_normal = data_profile_store.compute_psi(df_normal, "val", boundaries)
        psi_shifted = data_profile_store.compute_psi(df_shifted, "val", boundaries)

        # Normal PSI should be very small
        self.assertLess(psi_normal, 0.1)
        # Shifted PSI should be significant
        self.assertGreater(psi_shifted, 0.25)

    from unittest.mock import patch

    @patch('requests.post')
    def test_decision_tree_rule_induction(self, mock_post):
        # Setup mock response for requests.post
        from unittest.mock import MagicMock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": {"hits": []}}
        mock_post.return_value = mock_response

        # Create clean dataset (mostly low values)
        clean_data = [(float(i), 10.0) for i in range(100)]
        # Create quarantined dataset (mostly high values/outliers)
        quar_data = [(float(i + 80), 50.0) for i in range(20)]
        
        schema = StructType([
            StructField("age", DoubleType(), True),
            StructField("score", DoubleType(), True)
        ])
        
        clean_df = self.spark.createDataFrame(clean_data, schema)
        quar_df = self.spark.createDataFrame(quar_data, schema)

        advisor = AIRuleAdvisor(api_key="", model="llama-3.3-70b-versatile")
        # Run rule induction
        result = advisor.induce_rules_from_data(
            self.spark, clean_df, quar_df, ["age", "score"], "users", "test_run_123"
        )

        self.assertNotIn("error", result)
        self.assertIn("induced_rules", result)
        self.assertGreater(len(result["induced_rules"]), 0)
        
        # Check rule format
        rule = result["induced_rules"][0]
        self.assertIn("condition", rule)
        self.assertEqual(rule["action"], "quarantine")
        # Decision tree splits on whichever column provides best split (often score in this dataset)
        self.assertTrue(any(feat in rule["condition"] for feat in ["age", "score"]), 
                        f"Expected condition to mention age or score, got: {rule['condition']}")

if __name__ == "__main__":
    unittest.main()
