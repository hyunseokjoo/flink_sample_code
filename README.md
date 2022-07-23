# flink_sample_code
설치법 
[다운로드 주소](https://flink.apache.org/downloads.html)
다운로드 후 압축 풀기(tar파일)

pyflink conda에 설치
```bash
pip install apache-flink
```

클러스터 실행 & 작동 해제 
```bash
./bin/start-cluster.sh 
./bin/stop-cluster.sh
```

클러스터 실행 확인 
```bash
ps aux | grep flink
```
example 코드 돌려
```bash
# java 
./bin/flink run examples/streaming/WordCount.jar
# python 
./bin/flink run --python examples/python/table/word_count.py
```

ui확인법
http://localhost:8081



