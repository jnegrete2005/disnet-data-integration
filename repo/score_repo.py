from domain.models import Score

from repo.base import sql_op
from repo.generic_repo import GenericRepo


class ScoreRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)
        self.score_cache: dict[str, int] = {}

    @sql_op
    def __create_score_table(self, cursor) -> bool:
        query = """
            CREATE TABLE IF NOT EXISTS score (
                score_id INT AUTO_INCREMENT PRIMARY KEY,
                score_name VARCHAR(30) UNIQUE NOT NULL
            );
        """
        cursor.execute(query)
        return True

    def create_tables(self) -> bool:
        if not self.__create_score_table():
            return False
        return True

    @sql_op
    def get_or_create_score(self, cursor, score_name: str) -> int:
        if score_name in self.score_cache:
            return self.score_cache[score_name]

        select_query = """
            SELECT score_id FROM score
            WHERE score_name = %s;
        """
        cursor.execute(select_query, (score_name,))
        result = cursor.fetchone()
        if result:
            self.score_cache[score_name] = result[0]
            return result[0]

        insert_query = """
            INSERT INTO score (score_name)
            VALUES (%s);
        """
        cursor.execute(insert_query, (score_name,))
        self.score_cache[score_name] = cursor.lastrowid
        return cursor.lastrowid
