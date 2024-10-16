# Text2SQL
This was a fun project for me to do, but work needs to be done to clean it up. I'm a big fan of the NBA, so I created a chatbot using Google Gemini. The chatbot is designed to answer natural language queries by a user who is interested in knowing about NBA player stats. 

## Data Collection
I collected data via the NBA API, and saved it to an Amazon S3 bucket. I then stored the S3 data into an Amazon RDS Postgres database, and prompted the chatbot to query from that database to output answers.

## Learnings
I learned more about cloud technologies and how data is collected from a non-Kaggle/csv file source. This experience helped me significantly in my data science internships moving forward, as I was querying data from SQL databses daily.
I also was able to learn the basic structure of what goes on inside a chatbot, and the importance of prompt engineering.

## I aim to significantly increase the breadth of this project to cover more data, and to do some data modeling as well. The readme will also be revamped.
