import sqlite3
from datetime import date
from typing import Optional, List, Dict, Any


class StockEstimatesDB:
    def __init__(self, db_path: str = 'stock_estimates.db'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self._connect()
        self._create_table()

    def _connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def _create_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_estimates (
            date TEXT,
            symbol TEXT,
            price REAL,
            estimateCurrency TEXT,
            numberOfAnalysts INTEGER,
            recommendationRate REAL,
            recommendation TEXT,
            numberOfPriceTargets INTEGER,
            meanPriceTarget REAL,
            highPriceTarget REAL,
            lowPriceTarget REAL,
            medianPriceTarget REAL,
            strongBuy INTEGER,
            sell INTEGER,
            hold INTEGER,
            buy INTEGER,
            underperform INTEGER,
            consensusPriceVolatility TEXT,
            dateLastUpdated TEXT,
            industryDateLastUpdated TEXT,
            pricevolatilityDateLastUpdated TEXT,
            consensusIndustryRecommendation TEXT,
            rawResponse TEXT
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def insert_estimate(self, estimate: Dict[str, Any]):
        columns = ', '.join(estimate.keys())
        placeholders = ', '.join('?' * len(estimate))
        values = tuple(estimate.values())
        insert_sql = f"INSERT INTO stock_estimates ({columns}) VALUES ({placeholders})"
        self.cursor.execute(insert_sql, values)
        self.conn.commit()

    def select_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        select_sql = "SELECT * FROM stock_estimates WHERE symbol = ?"
        self.cursor.execute(select_sql, (symbol,))
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def select_all(self) -> List[Dict[str, Any]]:
        select_sql = "SELECT * FROM stock_estimates"
        self.cursor.execute(select_sql)
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def select_with_filters(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        where_clauses = []
        values = []
        for key, value in filters.items():
            where_clauses.append(f"{key} = ?")
            values.append(value)
        where_sql = " AND ".join(where_clauses)
        select_sql = f"SELECT * FROM stock_estimates WHERE {where_sql}"
        self.cursor.execute(select_sql, tuple(values))
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()