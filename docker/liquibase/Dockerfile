FROM liquibase/liquibase:4.31.0

WORKDIR /app

USER root

COPY install_liquibase_databricks.sh /

RUN /install_liquibase_databricks.sh

COPY liquibase.sh /

USER liquibase

ENTRYPOINT [ "/liquibase.sh" ]
