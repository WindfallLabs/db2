# !/usr/bin/env python2
"""
Classes for interactively exploring tables.
"""

import pandas as pd
from sqlalchemy import select, func, MetaData

from utils import df_to_prettytable


__all__ = ["TableSchema"]


class TableSchema(object):
    def __init__(self, database, table_name):
        """Provides an at-a-glace description of a table."""
        self.name = table_name
        self._d = database

    @property
    def Table(self):
        """SQLAlchemy Table object."""
        return self._d.schema.meta.tables[self.name]

    @property
    def table_schema(self):
        cols = ["Column", "Type", "Foreign Key", "Reference Keys"]
        schema = pd.DataFrame([], columns=cols)
        ref = self._d.schema.foreign_keys()

        for column in self.Table.columns:
            fkeys = list(column.foreign_keys)
            for_key = ""
            if fkeys:
                for_key = fkeys[0].target_fullname

            try:
                ref_keys = ", ".join(
                    ref[(ref["Foreign Table"] == self.name)
                        & (ref["Column"] == column.name)].apply(
                        lambda x: ".".join(x[["Table", "Column"]]),
                        axis=1).tolist())
            except AttributeError:
                ref_keys = ""

            # print("{} {} {}".format(column.name, column.type, fkey))
            row = pd.DataFrame([
                [column.name,
                 column.type,
                 for_key,
                 ref_keys]],
                columns=cols)
            schema = schema.append(row)
        return schema.reset_index(drop=True)

    def head(self):
        return self.all().head()

    def pretty(self):
        print(self.__str__())
        return

    @property
    def columns(self):
        return [c.name for c in self.Table.columns]

    @property
    def count(self):
        s = select([func.count()]).select_from(self.Table)
        s.bind = self._d.engine
        return s.execute().fetchone()[0]

    def __repr__(self):
        r = "<TableSchema for {tbl}: {rowcnt} Rows, {colcnt} Columns: {cols}>"
        col_cnt = len(self.columns)
        columns = self.columns
        if col_cnt > 5:
            columns = self.columns[:4]
            columns.extend(["..."])
        return r.format(
            tbl=self.name,
            rowcnt=self.count,
            colcnt=col_cnt,
            cols=columns)

    def __str__(self):
        return df_to_prettytable(self.table_schema, self.name)


class Schema(object):  # TODO: use Inspector class and refactor
    def __init__(self, database):
        """
        A subclass of sqlalchemy's Inspector class, which performs database
        schema inspection. This subclass also acts as a container for table
        schema objects.
        """
        # TODO: super(Schema, self).__init__(self, database.engine)
        self._d = database
        self._loaded = False

    def refresh(self):
        """Refreshes the schema."""
        if self._d._echo:
            print("Loading {} tables...".format(len(self._d.table_names)))
        # Set/Reset MetaData
        self.meta = MetaData(bind=self._d.engine)
        if self._d.dbtype == "mssql":
            #self.meta.schema = "dbo"
            self.meta.reflect(bind=self._d.engine, schema=self._d.schema_name)
        else:
            self.meta.reflect(bind=self._d.engine)

        for table_name in self._d.table_names:
            attr_name = table_name
            # Handle MSSQL names (e.g. 'dbo.MyTable' -> 'MyTable')
            if "." in table_name:
                attr_name = table_name.split(".")[1]
            setattr(self, attr_name, TableSchema(self._d, table_name))
        if not self._loaded:
            self._loaded = True
        return

    def foreign_keys(self):
        """Returns a DataFrame of foreign key relationships."""
        ref_cols = ["Table", "Column", "Foreign Table", "Foreign Key"]
        reference_df = pd.DataFrame([], columns=ref_cols)
        for table_name in self._d.table_names:
            try:
                tbl = self.meta.tables[table_name]
                for const in tbl.foreign_key_constraints:
                    for col_name, column in const.columns.items():
                        target = [fk.target_fullname for fk
                                  in column.foreign_keys][0]
                        f_table, f_col = target.split(".")
                        row = [table_name, col_name, f_table, f_col]
                        reference_df = reference_df.append(
                            pd.DataFrame([row], columns=ref_cols))
            except KeyError:
                pass
        return reference_df.reset_index(drop=True)

    def __str__(self):
        s = "<Schema ({}): {}>"
        if not self._loaded:
            return s.format(self._d.dbname, "NOT LOADED")
        tbl_cnt = len([v for v in self.__dict__.values()
                       if type(v).__name__ == "TableSchema"])
        return s.format(self._d.dbname,
                        "{} Tables Loaded".format(tbl_cnt))

    def __repr__(self):
        return self.__str__()
