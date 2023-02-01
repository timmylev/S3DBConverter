import os


# Ensure that boto3 never grabs our actual credentials incase mock is not started.
# https://github.com/spulec/moto#very-important----recommended-usage
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_REGION"] = "us-east-1"
