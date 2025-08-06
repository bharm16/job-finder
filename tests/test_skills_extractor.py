import pytest
from parsers.skills_extractor import extract_skills, extract_skills_with_categories, SkillsExtractor


def test_extract_programming_languages():
    """Test extraction of programming language skills."""
    text = "Looking for a developer with Python, JavaScript, and Java experience"
    skills = extract_skills(text)
    
    assert "Python" in skills
    assert any("javascript" in s.lower() for s in skills)
    assert "Java" in skills


def test_extract_web_technologies():
    """Test extraction of web framework and technology skills."""
    text = "Must have experience with React, Angular, Node.js, and Django"
    skills = extract_skills(text)
    
    assert "React" in skills
    assert "Angular" in skills
    assert any("node.js" in s.lower() for s in skills)
    assert "Django" in skills


def test_extract_databases():
    """Test extraction of database skills."""
    text = "Experience with PostgreSQL, MongoDB, and Redis required"
    skills = extract_skills(text)
    
    assert any("postgresql" in s.lower() for s in skills)
    assert any("mongodb" in s.lower() for s in skills)
    assert any("redis" in s.lower() for s in skills)


def test_extract_cloud_devops():
    """Test extraction of cloud and DevOps skills."""
    text = "AWS, Docker, Kubernetes, and CI/CD experience needed"
    skills = extract_skills(text)
    
    assert any("aws" in s.lower() or "amazon web services" in s.lower() for s in skills)
    assert any("docker" in s.lower() for s in skills)
    assert any("kubernetes" in s.lower() for s in skills)


def test_extract_with_years_pattern():
    """Test extraction of skills mentioned with years of experience."""
    text = "5+ years Python, 3 years React, 2+ years AWS experience"
    skills = extract_skills(text)
    
    assert any("python" in s.lower() for s in skills)
    assert any("react" in s.lower() for s in skills)
    assert any("aws" in s.lower() or "amazon web services" in s.lower() for s in skills)


def test_extract_aliases():
    """Test that common aliases are recognized."""
    text = "Experience with JS, TS, K8s, and GCP"
    skills = extract_skills(text)
    
    # Should resolve aliases to full names
    assert any("javascript" in s.lower() for s in skills)
    assert any("typescript" in s.lower() for s in skills)
    assert any("kubernetes" in s.lower() for s in skills)
    assert any("google cloud" in s.lower() for s in skills)


def test_extract_multi_word_skills():
    """Test extraction of multi-word skills."""
    text = "Experience with machine learning, Google Cloud Platform, and Ruby on Rails"
    skills = extract_skills(text)
    
    assert any("machine learning" in s.lower() for s in skills)
    assert any("google cloud" in s.lower() or "gcp" in s.lower() for s in skills)
    assert any("ruby on rails" in s.lower() or "rails" in s.lower() for s in skills)


def test_empty_input():
    """Test that empty input returns empty list."""
    assert extract_skills("") == []
    assert extract_skills(None) == []


def test_no_skills_in_text():
    """Test text with no recognizable skills."""
    text = "We are looking for a motivated individual to join our team"
    skills = extract_skills(text)
    
    # Might extract soft skills like "teamwork" if in taxonomy
    # or might be empty if no skills found
    assert isinstance(skills, list)


def test_categorized_extraction():
    """Test categorized skill extraction."""
    text = """
    Senior Full-Stack Developer needed:
    - Python, JavaScript, TypeScript
    - React, Django, Node.js
    - PostgreSQL, MongoDB
    - AWS, Docker, Kubernetes
    - Machine Learning with TensorFlow
    - Git, Agile, Leadership skills
    """
    
    categorized = extract_skills_with_categories(text)
    
    assert "languages" in categorized
    assert "web" in categorized
    assert "databases" in categorized
    assert "cloud_devops" in categorized
    
    # Check some specific categorizations
    assert any("python" in s.lower() for s in categorized.get("languages", []))
    assert any("react" in s.lower() for s in categorized.get("web", []))
    assert any("postgresql" in s.lower() for s in categorized.get("databases", []))


def test_case_insensitive_matching():
    """Test that skill matching is case-insensitive."""
    text = "PYTHON, python, Python, pYtHoN"
    skills = extract_skills(text)
    
    # Should extract Python only once (deduplication)
    python_count = sum(1 for s in skills if s.lower() == "python")
    assert python_count == 1


def test_html_tag_removal():
    """Test that HTML tags are properly removed before extraction."""
    text = "<p>Looking for <strong>Python</strong> and <em>JavaScript</em> developers</p>"
    skills = extract_skills(text)
    
    assert "Python" in skills
    assert "Javascript" in skills


def test_special_characters_handling():
    """Test handling of skills with special characters."""
    text = "C++, C#, F#, ASP.NET, Node.js experience required"
    skills = extract_skills(text)
    
    # These might be normalized differently, so check presence in some form
    assert any("c++" in s.lower() for s in skills)
    assert any("c#" in s.lower() for s in skills)
    assert any("f#" in s.lower() for s in skills)
    assert any("asp.net" in s.lower() for s in skills)


def test_skill_deduplication():
    """Test that duplicate skills are removed."""
    text = "Python Python Python Java Java JavaScript JavaScript"
    skills = extract_skills(text)
    
    # Each skill should appear only once
    assert skills.count("Python") == 1
    assert skills.count("Java") == 1
    assert skills.count("Javascript") == 1


def test_real_job_description():
    """Test with a realistic job description."""
    job_description = """
    We are seeking a Senior Software Engineer to join our growing team.
    
    Requirements:
    • 5+ years of experience with Python or Java
    • Strong knowledge of RESTful APIs and microservices architecture
    • Experience with cloud platforms (AWS, GCP, or Azure)
    • Proficiency in SQL and NoSQL databases (PostgreSQL, MongoDB)
    • Familiarity with containerization (Docker, Kubernetes)
    • Experience with CI/CD pipelines and GitHub Actions
    • Understanding of Agile methodologies
    • Excellent problem-solving and communication skills
    
    Nice to have:
    • Machine learning experience with TensorFlow or PyTorch
    • Front-end experience with React or Angular
    • Knowledge of event-driven architecture and Kafka
    """
    
    skills = extract_skills(job_description)
    
    # Should extract many skills
    assert len(skills) > 10
    
    # Check for key skills
    assert "Python" in skills
    assert "Java" in skills
    assert any("aws" in s.lower() or "amazon web services" in s.lower() for s in skills)
    assert any("docker" in s.lower() for s in skills)
    assert any("postgresql" in s.lower() for s in skills)


if __name__ == "__main__":
    # Run a simple test
    test_text = """
    We're looking for a Full-Stack Developer with:
    - 3+ years Python and JavaScript
    - React.js and Node.js experience  
    - AWS and Docker knowledge
    - PostgreSQL and MongoDB
    - Strong problem-solving skills
    """
    
    print("Testing skills extraction...")
    skills = extract_skills(test_text)
    print(f"Found {len(skills)} skills:")
    for skill in skills:
        print(f"  - {skill}")
    
    print("\nCategorized skills:")
    categorized = extract_skills_with_categories(test_text)
    for category, category_skills in categorized.items():
        print(f"\n{category}:")
        for skill in category_skills:
            print(f"  - {skill}")