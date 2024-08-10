from openai import OpenAI
client = OpenAI()

completion = client.chat.completions.create(
  model="gpt-4o-mini",
  messages=[
    {"role": "system", "content": "Generate code in python only. If you want to explain something do it in comments."},
    {"role": "user", "content": "Generate hello world script."}
  ]
)

print(completion.choices[0].message.content)