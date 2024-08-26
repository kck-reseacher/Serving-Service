FROM aiops/srs:backbone

COPY ./ /root/ai-module/mlops-srs

WORKDIR /root/ai-module/mlops-srs
RUN chmod -R 777 ./*