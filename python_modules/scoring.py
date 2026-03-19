# Computes BabyQ score using fixed points for each question
# Expects a list of 15 "Yes"/"No" strings

POINTS = [
    19,  # Q1: Alcohol
    19,  # Q2: Smoking
    6,   # Q3: Sleep
    4,   # Q4: Prenatal checkups
    14,  # Q5: Exercise
    4,   # Q6: Protein
    4,   # Q7: Sugar
    6,   # Q8: Depression/Hope
    4,   # Q9: Vegetables
    4,   # Q10: Social support
    2,   # Q11: Stress management
    2,   # Q12: Dental care
    4,   # Q13: Prenatal vitamins
    4,   # Q14: Dairy
    4    # Q15: Whole grains
]

def calculate_score(answers):
    """
    Parameters:
        answers (list of str): A list of 15 responses, each either 'Yes' or 'No'

    Returns:
        int: Total BabyQ score based on fixed point values
    """
    if not isinstance(answers, list) or len(answers) != 15:
        raise ValueError("Expected a list of exactly 15 'Yes' or 'No' answers.")

    total = 0
    for answer, points in zip(answers, POINTS):
        if answer.strip().lower() == "yes":
            total += points
    return total
