FROM public.ecr.aws/lambda/python:3.12 AS builder

WORKDIR /build
COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && pip wheel --no-cache-dir --wheel-dir /tmp/wheels -r requirements.txt

FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
COPY --from=builder /tmp/wheels /tmp/wheels
RUN python -m pip install --no-cache-dir --no-compile --no-index --find-links=/tmp/wheels -r "${LAMBDA_TASK_ROOT}/requirements.txt" \
    && rm -rf /tmp/wheels

COPY data_slacklake ${LAMBDA_TASK_ROOT}/data_slacklake
COPY main.py ${LAMBDA_TASK_ROOT}
COPY worker.py ${LAMBDA_TASK_ROOT}

CMD ["main.handler"]
