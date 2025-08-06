from typing import List, Set, Optional
import re
import sys
import spacy
from spacy.matcher import PhraseMatcher

# Common software engineering skills taxonomy
SKILLS_TAXONOMY = {
    # Programming Languages
    "languages": [
        "python", "java", "javascript", "typescript", "c++", "c#", "ruby", "go", "golang",
        "rust", "swift", "kotlin", "scala", "php", "perl", "r", "matlab", "julia",
        "objective-c", "dart", "elixir", "clojure", "haskell", "f#", "vb.net", "cobol",
        "fortran", "pascal", "lua", "groovy", "shell", "bash", "powershell"
    ],
    # Web Technologies
    "web": [
        "html", "css", "sass", "less", "react", "reactjs", "angular", "angularjs", "vue",
        "vuejs", "svelte", "next.js", "nextjs", "nuxt", "gatsby", "webpack", "babel",
        "node.js", "nodejs", "express", "expressjs", "django", "flask", "fastapi",
        "rails", "ruby on rails", "asp.net", "spring", "spring boot", "laravel", "symfony"
    ],
    # Databases
    "databases": [
        "sql", "nosql", "postgresql", "postgres", "mysql", "mariadb", "sqlite", "oracle",
        "mongodb", "cassandra", "redis", "elasticsearch", "dynamodb", "firestore",
        "neo4j", "graphql", "couchdb", "influxdb", "timescaledb", "cockroachdb"
    ],
    # Cloud & DevOps
    "cloud_devops": [
        "aws", "amazon web services", "azure", "gcp", "google cloud", "docker", "kubernetes",
        "k8s", "terraform", "ansible", "jenkins", "github actions", "gitlab ci", "circleci",
        "travis ci", "cloudformation", "helm", "vagrant", "puppet", "chef", "saltstack",
        "prometheus", "grafana", "datadog", "new relic", "splunk", "elk stack"
    ],
    # Data & ML
    "data_ml": [
        "machine learning", "deep learning", "artificial intelligence", "ai", "ml",
        "tensorflow", "pytorch", "keras", "scikit-learn", "sklearn", "pandas", "numpy",
        "scipy", "matplotlib", "seaborn", "plotly", "jupyter", "spark", "hadoop",
        "airflow", "kafka", "flink", "storm", "hive", "presto", "dbt", "tableau",
        "power bi", "looker", "data science", "nlp", "computer vision", "opencv"
    ],
    # Mobile
    "mobile": [
        "android", "ios", "react native", "flutter", "xamarin", "ionic", "cordova",
        "swift ui", "swiftui", "jetpack compose", "kotlin multiplatform"
    ],
    # Testing & QA
    "testing": [
        "unit testing", "integration testing", "e2e testing", "jest", "mocha", "jasmine",
        "pytest", "unittest", "selenium", "cypress", "playwright", "puppeteer",
        "junit", "testng", "rspec", "cucumber", "tdd", "bdd", "qa", "quality assurance"
    ],
    # Architecture & Patterns
    "architecture": [
        "microservices", "serverless", "rest", "restful", "graphql", "grpc", "soap",
        "event-driven", "domain-driven design", "ddd", "cqrs", "event sourcing",
        "design patterns", "solid principles", "clean architecture", "mvc", "mvvm",
        "mvi", "hexagonal architecture", "onion architecture", "12-factor"
    ],
    # Version Control & Collaboration
    "collaboration": [
        "git", "github", "gitlab", "bitbucket", "svn", "mercurial", "agile", "scrum",
        "kanban", "jira", "confluence", "slack", "teams", "asana", "trello"
    ],
    # Security
    "security": [
        "oauth", "jwt", "ssl", "tls", "encryption", "cryptography", "penetration testing",
        "owasp", "security scanning", "vulnerability assessment", "soc2", "gdpr",
        "pci compliance", "identity management", "iam", "rbac", "zero trust"
    ],
    # Blockchain & Web3
    "blockchain": [
        "blockchain", "ethereum", "solidity", "web3", "smart contracts", "defi",
        "nft", "ipfs", "metamask", "truffle", "hardhat", "ganache"
    ],
    # Soft Skills (relevant for tech roles)
    "soft_skills": [
        "leadership", "mentoring", "communication", "problem-solving", "teamwork",
        "project management", "stakeholder management", "technical writing",
        "code review", "pair programming", "remote work", "cross-functional"
    ]
}


class SkillsExtractor:
    def __init__(self, model_name: str = "en_core_web_sm"):
        """Initialize the skills extractor with spaCy model."""
        try:
            self.nlp = spacy.load(model_name)
        except OSError:
            # If model not found, download it
            import subprocess
            subprocess.run([sys.executable, "-m", "spacy", "download", model_name])
            self.nlp = spacy.load(model_name)

        # Initialize phrase matcher for multi-word skills
        self.matcher = PhraseMatcher(self.nlp.vocab, attr="LOWER")

        # Add all skills to the matcher
        all_skills = []
        for category_skills in SKILLS_TAXONOMY.values():
            all_skills.extend(category_skills)

        # Create patterns for multi-word skills
        patterns = [self.nlp.make_doc(skill) for skill in all_skills]
        self.matcher.add("SKILLS", patterns)

        # Create a set for fast single-word lookup
        self.skill_set = {skill.lower() for skill in all_skills}

        # Common skill variations and aliases
        self.aliases = {
            "js": "javascript",
            "ts": "typescript",
            "py": "python",
            "node": "node.js",
            "psql": "postgresql",
            "k8s": "kubernetes",
            "gcp": "google cloud",
            "aws": "amazon web services",
            "ml": "machine learning",
            "ai": "artificial intelligence",
            "ci/cd": "continuous integration",
            "devops": "devops",
            "frontend": "front-end",
            "backend": "back-end",
            "fullstack": "full-stack",
            "ui/ux": "user interface design"
        }

    def extract_skills(self, text: str) -> List[str]:
        """Extract skills from job description text."""
        if not text:
            return []

        # Clean and preprocess text
        text = self._preprocess_text(text)

        # Process with spaCy
        doc = self.nlp(text.lower())

        found_skills = set()

        # Find multi-word skills using PhraseMatcher
        matches = self.matcher(doc)
        for match_id, start, end in matches:
            skill = doc[start:end].text
            found_skills.add(self._normalize_skill(skill))

        # Find single-word skills in tokens
        for token in doc:
            if token.text in self.skill_set:
                found_skills.add(self._normalize_skill(token.text))

        # Check for skill aliases
        for alias, canonical in self.aliases.items():
            if alias in text.lower():
                found_skills.add(canonical)

        # Look for years of experience patterns (e.g., "3+ years Python")
        experience_pattern = r'(\d+\+?\s*(?:years?|yrs?)?\s*(?:of\s*)?)([\w\s\+\#\.]+)'
        for match in re.finditer(experience_pattern, text, re.IGNORECASE):
            potential_skill = match.group(2).strip().lower()
            if potential_skill in self.skill_set:
                found_skills.add(self._normalize_skill(potential_skill))

        # Normalize all skills and remove duplicates
        normalized_skills = set()
        for skill in found_skills:
            normalized = self._normalize_skill(skill)
            normalized_skills.add(normalized)
        
        return sorted(list(normalized_skills))

    def _preprocess_text(self, text: str) -> str:
        """Clean and prepare text for processing."""
        # Remove HTML tags if present
        text = re.sub(r'<[^>]+>', ' ', text)
        # Replace common separators with spaces
        text = re.sub(r'[/\-_]', ' ', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _normalize_skill(self, skill: str) -> str:
        """Normalize skill name for consistency."""
        skill = skill.lower().strip()
        # Handle common variations
        if skill in self.aliases:
            return self.aliases[skill]
        # Capitalize appropriately
        if '.' in skill:
            # Handle things like "node.js" -> "Node.js"
            parts = skill.split('.')
            return '.'.join(p.capitalize() if i == 0 else p for i, p in enumerate(parts))
        elif skill.upper() in ['aws', 'gcp', 'api', 'sql', 'nosql', 'html', 'css', 'xml',
                               'json', 'jwt', 'ssl', 'tls', 'iam', 'rbac', 'gdpr', 'grpc']:
            return skill.upper()
        else:
            # Title case for most skills
            return ' '.join(word.capitalize() for word in skill.split())

    def extract_skills_with_categories(self, text: str) -> dict:
        """Extract skills and categorize them."""
        skills = self.extract_skills(text)
        categorized = {category: [] for category in SKILLS_TAXONOMY.keys()}

        for skill in skills:
            skill_lower = skill.lower()
            for category, category_skills in SKILLS_TAXONOMY.items():
                if skill_lower in [s.lower() for s in category_skills]:
                    categorized[category].append(skill)
                    break

        # Remove empty categories
        return {k: v for k, v in categorized.items() if v}


# Module-level instance for convenience
_extractor = None


def get_extractor() -> SkillsExtractor:
    """Get or create a singleton SkillsExtractor instance."""
    global _extractor
    if _extractor is None:
        _extractor = SkillsExtractor()
    return _extractor


def extract_skills(text: str) -> List[str]:
    """Extract skill keywords from the given text.

    This is the main function to be used by other modules.
    """
    extractor = get_extractor()
    return extractor.extract_skills(text)


def extract_skills_with_categories(text: str) -> dict:
    """Extract skills and return them categorized."""
    extractor = get_extractor()
    return extractor.extract_skills_with_categories(text)


if __name__ == "__main__":
    # Test the extractor
    sample_job = """
    We are looking for a Senior Full-Stack Engineer with 5+ years of experience.
    Required skills:
    - Strong proficiency in Python, JavaScript/TypeScript, and React.js
    - Experience with AWS, Docker, and Kubernetes
    - PostgreSQL and MongoDB database experience
    - Knowledge of RESTful APIs and microservices architecture
    - Experience with CI/CD pipelines using GitHub Actions
    - Machine learning experience with TensorFlow or PyTorch is a plus
    - Excellent problem-solving and communication skills
    """

    print("Extracted skills:")
    skills = extract_skills(sample_job)
    for skill in skills:
        print(f"  - {skill}")

    print("\nCategorized skills:")
    categorized = extract_skills_with_categories(sample_job)
    for category, skills in categorized.items():
        print(f"\n{category.replace('_', ' ').title()}:")
        for skill in skills:
            print(f"  - {skill}")