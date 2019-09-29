# Have Fun With Stock Data from Naver Finance


## preface
머신러닝으로 돈 벌기가 좋은 게 어디 있을까 항상 생각하지만 증권 예측처럼 머리에 계속 맴도는 게 없습니다. 
  
가능하기만 하다면 너무 좋은 문제이고 데이터도 확보하기가 쉬운 편이니 한 번 재미삼아 해보려 합니다.  


## blog like changelog

`2019-09-04`

순수한 호기심으로 소스를 드문드문 짜보다가 기관 투자자 데이터 스크랩핑을 완성했습니다.
데이터 저장소에 대한 고민은 더 필요하지만, 1년 미만의 작은 데이터를 커버하기엔 무리 없을 것 같습니다.
드문드문 소스 확인하고 데이터 수집 이후에 간단한 로직을 만들어보고 괜찮다 싶으면 공유할 생각입니다.
  
아, 폴더 가운데 `algo`는 algorithm의 줄임말입니다.

`2019-09-28`

데이터 수집을 하다보니 기존 pickle로 저장하는 방식이 cpu 점유율을 올리는 문제가 있었습니다.
따라서 sqlite 기반의 [sqlitedict](https://anaconda.org/conda-forge/sqlitedict)로 데이터 저장 방법을 변경했습니다.

conda 환경 공유에 필요한 `environment.yml`을 생성하는 명령어를 `conda env export --from-history`로 변경했습니다.