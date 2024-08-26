import peewee as pw
from peewee_async import Manager, PostgresqlDatabase

# 비동기 PostgreSQL 데이터베이스 연결 설정
db = PostgresqlDatabase(
    'postgres',  # 데이터베이스 이름
    user='postgres',  # 사용자 이름
    password='1',  # 비밀번호
    host='10.10.34.20',  # 호스트 주소
    port=5432  # 포트 번호
)

# 모델 정의
class AiResultGdnSummary(pw.Model):
    seq = pw.BigIntegerField(primary_key=True)  # seq 필드, 기본키로 설정
    time = pw.DateTimeField(null=False)  # "time" 필드
    target_id = pw.CharField(max_length=300, null=False)  # target_id 필드
    inst_type = pw.CharField(null=False)  # inst_type 필드
    normality_score = pw.FloatField(null=True)  # normality_score 필드
    is_anomaly = pw.BooleanField(null=False)  # is_anomaly 필드
    target_grade = pw.CharField(null=True)  # target_grade 필드

    class Meta:
        database = db  # 모델이 사용할 데이터베이스 설정
        table_name = 'ai_result_gdn_summary'  # 테이블 이름
        indexes = (
            (('time', 'target_id', 'inst_type'), True),  # 유니크 제약 조건
        )

# 비동기 매니저 설정
objects = Manager(db)

def connect_db():
    # 데이터베이스 연결
    objects.database.connect()

def close_db():
    # 데이터베이스 연결 해제
    objects.database.close()

def gdn_summary_create(data):

    try:
        # 데이터 insert
        with objects.database.atomic():
            example_instance = AiResultGdnSummary.create(
                time=data['time'],
                target_id=data['target_id'],
                inst_type=data['inst_type'],
                normality_score=data['normality_score'],
                is_anomaly=data['is_anomaly'],
                target_grade=data['target_grade']
            )
        print(f"Inserted: {example_instance.seq}, {example_instance.time}")
    except pw.IntegrityError as e:
        print(f"Failed to insert record: {e}")

if __name__ == "__main__":
    gdn_summary_create()
