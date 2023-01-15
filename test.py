from datetime import datetime

a = datetime.now().isoformat()
EXEC_DATE = "{{ execution_date.strftime('%Y-%m-%d %H:%M:S') }}"
print(a)