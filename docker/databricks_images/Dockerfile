FROM databricksruntime/standard:16.4-LTS

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

COPY requirements.in .
RUN /databricks/python3/bin/pip install -r requirements.in
