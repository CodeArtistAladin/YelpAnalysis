import openai

# Initialize ChatGPT API
openai.api_key = "sk-abc123xyz456def789ghi012jkl345mno678pqr901stu234"


def generate_recommendation_explanation(user_id, business_id):
    """
    Generate a natural language explanation for business recommendations.
    """
    business_info = business_df.filter(
        business_df.business_id == business_id
    ).first()

    prompt = f"""
    Recommend {business_info.name} to user {user_id}. 
    It’s a {business_info.categories} business rated {business_info.stars} stars, 
    known for {business_info.attributes}. Explain why it’s a great match.
    """

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content
