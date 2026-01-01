from infraestructure.database import DisnetManager


class GenericRepo:
    def __init__(self, db: DisnetManager):
        self.db = db
