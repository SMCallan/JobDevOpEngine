import unittest

from scraper import classify_apply_action, classify_culture, score_job


class ScoringIntelligenceTests(unittest.TestCase):
    def test_high_fit_role_gets_apply_now_tag(self):
        job = {
            "id": "sample_1",
            "title": "Secure Full Stack Engineer",
            "company": "GitHub",
            "salary": "£70,000 - £85,000",
            "salary_min": 70000,
            "salary_max": 85000,
            "link": "https://example.com/apply",
            "description": (
                "Build Python, TypeScript, React and API features with Docker, "
                "CI/CD, OWASP and secure SDLC practices. Flexible working and mentorship."
            ),
        }

        scored = score_job(job)

        self.assertEqual(scored["action_recommendation"], "Apply now")
        self.assertEqual(scored["action_urgency"], 3)
        self.assertIn("apply-now", scored["tags"])

    def test_culture_risk_blocks_apply_now(self):
        culture_risk, culture_score, reasons = classify_culture(
            "Platform Engineer",
            "Urgent requirement in a high-pressure 24/7 team with weekend work and out of hours support.",
        )

        self.assertEqual(culture_risk, "High culture-risk")
        self.assertLess(culture_score, 8)
        self.assertTrue(any("Culture-risk terms" in reason for reason in reasons))

        recommendation, urgency, action_reasons, tags = classify_apply_action(
            fit_score=90,
            salary_band="Core target",
            culture_risk=culture_risk,
            has_link=True,
        )

        self.assertEqual(recommendation, "Research culture first")
        self.assertEqual(urgency, 1)
        self.assertIn("high-culture-risk", tags)
        self.assertTrue(action_reasons)

    def test_missing_apply_link_is_manual_review(self):
        recommendation, urgency, reasons, tags = classify_apply_action(
            fit_score=90,
            salary_band="Core target",
            culture_risk="Low culture-risk",
            has_link=False,
        )

        self.assertEqual(recommendation, "Review manually")
        self.assertEqual(urgency, 1)
        self.assertIn("manual-review", tags)
        self.assertTrue(reasons)


if __name__ == "__main__":
    unittest.main()
