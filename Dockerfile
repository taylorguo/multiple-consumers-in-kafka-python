FROM registry.qt6.cn/innotechxdc/base-cpc-classification:v0
ADD ./ ./
CMD ["python3.6", "app.py"]
