# base image for Docker from AWS
FROM public.ecr.aws/lambda/python:3.10

# libraries for our script
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY main.py ./

# run the code, entering in the "main" function
CMD ["main.lambda_handler"]