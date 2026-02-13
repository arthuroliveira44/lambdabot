FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

COPY data_slacklake ${LAMBDA_TASK_ROOT}/data_slacklake
COPY main.py ${LAMBDA_TASK_ROOT}
COPY worker.py ${LAMBDA_TASK_ROOT}


CMD [ "main.handler" ]
