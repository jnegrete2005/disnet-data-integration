from repo.base import sql_op
from repo.generic_repo import GenericRepo


class SourceRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)

    @sql_op
    def get_or_create_source(self, cursor, source_name: str) -> int:
        """
        Get the source_id for a given source_name. If it does not exist, create it.
        Returns the source_id.
        """
        select_query = "SELECT source_id FROM source WHERE name = %s;"
        cursor.execute(select_query, (source_name,))
        result = cursor.fetchone()
        if result:
            return result[0]

        insert_query = "INSERT INTO source (name) VALUES (%s);"
        cursor.execute(insert_query, (source_name,))
        return cursor.lastrowid
