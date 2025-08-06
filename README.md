# 가상환경 구성
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

# requirements.txt 만드려면
```
pip freeze > requirements.txt
```

```
nohup python3 kospi.py > output_files/logs/nohup.out 2>&1 &

nohup python3 snp500.py > output_files/logs/nohup.out 2>&1 &

```
