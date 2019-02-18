FROM python:3-alpine

RUN apk add --no-cache gcc
RUN apk add alpine-sdk

RUN pip install pyyaml
RUN pip install requests
RUN pip install datapackage
RUN pip install goodtables
RUN pip install biopython
RUN pip install elasticsearch==5.5.3

ENTRYPOINT ["python", "app.py"]
