import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class Database:
    def __init__(self, db_path):

        self.db_path = db_path

        def create_connection(path):
           

            conn = None

            try:
                conn = sqlite3.connect(path)
                return conn
            except sqlite3.Error as error:
                print(error)

            return conn

        self.conn = create_connection(self.db_path)

    def execute_query(self, query=None, get_results=False, get_first_item=False):
        

        try:
            c = self.conn.cursor()

            c.execute(query)

            if query is not None:
                if get_results == True:
                    if get_first_item == True:
                        
                        results = [item[0] for item in c.fetchall()]
                    else:
                        
                        results = [item for item in c.fetchall()]

                    return results
        except sqlite3.Error as error:
            print(error)
        finally:
            self.conn.commit()

    def create_table(self, table):
        

        title = table[0]
        table = table[1:]
        table_len = len(table)

        all_cols = ""
        for counter, col in enumerate(table):
            if table_len == counter + 1:
                
                all_cols += col
                break

            all_cols += col + ","

        create_table = f""" CREATE TABLE IF NOT EXISTS {title} (
                                        {all_cols}
                                    ); """

        self.execute_query(query=create_table)

    def drop_table(self, table_name):
        

        drop_table = f"DROP TABLE {table_name};"

        self.execute_query(query=drop_table)

    def init_db(self, table_name, file_path=None):
        

        df = pd.read_csv(file_path, na_values="---")
        df.to_sql(name=table_name, con=self.conn, if_exists="append", index=False)

        return df

    def create_dimension_table(self, fact_table, col_to_be_mapped, new_cols):
        

        unique_values_lst = fact_table[col_to_be_mapped].unique()
        ids = list(range(1, 1 + len(unique_values_lst)))

        dim_table = pd.DataFrame(
            data=list(zip(ids, unique_values_lst)), columns=new_cols
        )

        return dim_table

    def map_vals_to_ids(
        self,
        fact_df,
        fact_col_name,
        dimension_df,
        dimension_keycol_name,
        dimension_refcol_name,
    ):
        
        
        mapping_dict = dimension_df.set_index(dimension_refcol_name)[
            dimension_keycol_name
        ].to_dict()

        
        col_vals = fact_df.filter(like=fact_col_name)

        
        fact_df[col_vals.columns] = col_vals.replace(mapping_dict)

        return fact_df

    def fill_new_fields(
        self,
        df,
        idx,
        transactionTypeMsg,
        isCheckedOutNum,
        checkedOutMemberId,
        isReservedNum,
        reservedMemberId,
        initialDate,
        endRecordDate,
        isActive,
        gui_flag=False,
        bookId=None,
        get_df=False,
    ):
        

        if gui_flag == True:
            df.loc[idx, "TransactionId"] = idx
            df.loc[idx, "BookId"] = bookId

        df.loc[idx, "TransactionType"] = transactionTypeMsg
        df.loc[idx, "IsCheckedOut"] = isCheckedOutNum
        df.loc[idx, "CheckedOutMemberId"] = checkedOutMemberId
        df.loc[idx, "IsReserved"] = isReservedNum
        df.loc[idx, "ReservedMemberId"] = reservedMemberId

        reserve_or_checkout_flag = False
        if transactionTypeMsg == "Reserve":
            reserve_or_checkout_flag = True
            num_of_days = 10
        elif transactionTypeMsg == "Checkout":
            reserve_or_checkout_flag = True
            num_of_days = 30

        
        if reserve_or_checkout_flag == True:
            
            init_date = datetime.strptime(initialDate, "%d/%m/%Y")
            
            expiry_date = init_date + timedelta(days=num_of_days)
            
            expiry_date = expiry_date.strftime("%Y/%m/%d")
            
            expiry_date = str(expiry_date).split()[0]
            df.loc[idx, "TransactionTypeExpirationDate"] = expiry_date
        
        else:
            df.loc[idx, "TransactionTypeExpirationDate"] = np.nan

        df.loc[idx, "StartRecordDate"] = datetime.strptime(
            initialDate, "%d/%m/%Y"
        ).strftime("%Y/%m/%d")
        
        df.loc[idx, "IsActive"] = isActive

        
        if get_df == True:
            return df

    def deactivateLastTransaction(
        self, final_df, bookId, get_df_of_same_bookIds_via_gui=False
    ):
        
        
        allTransactions_of_bookId_df = final_df[final_df["BookId"] == bookId].dropna(
            axis=0, subset=["IsActive"]
        )

        last_activatedTransactionId = None
        
        if len(allTransactions_of_bookId_df) >= 1:
            if get_df_of_same_bookIds_via_gui == True:
                return allTransactions_of_bookId_df.iloc[-1]

            
            last_activatedTransactionId = allTransactions_of_bookId_df.iloc[-1][
                "TransactionId"
            ]

            
            final_df.loc[last_activatedTransactionId - 1, "IsActive"] = 0

        return last_activatedTransactionId

    def findLastMemberId_and_deactivateLastTransaction(
        self, initial_df, final_df, bookId, memberId, columnOfmemberId="MemberId"
    ):
        

        last_activatedTransactionId = self.deactivateLastTransaction(final_df, bookId)

        last_memberId = None
        if last_activatedTransactionId is not None:

            if (
                final_df.loc[last_activatedTransactionId - 1, "TransactionType"]
                == "Checkout"
            ):
                
                last_memberId = initial_df.loc[
                    last_activatedTransactionId - 1, columnOfmemberId
                ]

            elif (
                final_df.loc[last_activatedTransactionId - 1, "TransactionType"]
                == "Reserve"
            ):
                
                last_memberId = initial_df.loc[
                    last_activatedTransactionId - 1, columnOfmemberId
                ]

        
        if last_memberId is None:
            last_memberId = memberId

        
        
        return int(last_memberId)

    def create_empty_transactions_fact_table(self, df):
        
        transactions_df = pd.DataFrame()
        data = {
            "TransactionId": df["TransactionId"],
            "BookId": df["BookId"],
            "TransactionType": np.nan,
            "IsCheckedOut": np.nan,
            "CheckedOutMemberId": np.nan,
            "ReservedMemberId": np.nan,
            "IsReserved": np.nan,
            "TransactionTypeExpirationDate": np.nan,
            "StartRecordDate": np.nan,
            "EndRecordDate": np.nan,
            "IsActive": np.nan,
        }
        transactions_df = pd.DataFrame(
            data,
            columns=[
                "TransactionId",
                "BookId",
                "TransactionType",
                "IsCheckedOut",
                "CheckedOutMemberId",
                "IsReserved",
                "ReservedMemberId",
                "TransactionTypeExpirationDate",
                "StartRecordDate",
                "EndRecordDate",
                "IsActive",
            ],
        )

        return transactions_df

    def normalize_data(self, bookInfo_df, loanReservationHistory_df):

        
        bookInventory_df = bookInfo_df[["Id", "Genre", "Title", "Author"]].copy()

        
        genre_df = self.create_dimension_table(
            bookInventory_df, "Genre", ["GenreKey", "GenreRef"]
        )
        bookInventory_df = self.map_vals_to_ids(
            bookInventory_df, "Genre", genre_df, "GenreKey", "GenreRef"
        )

        
        bookTitles_df = self.create_dimension_table(
            bookInventory_df, "Title", ["BookTitleKey", "BookTitleRef"]
        )
        bookInventory_df = self.map_vals_to_ids(
            bookInventory_df, "Title", bookTitles_df, "BookTitleKey", "BookTitleRef"
        )

        
        bookAuthors_df = self.create_dimension_table(
            bookInventory_df, "Author", ["BookAuthorKey", "BookAuthorRef"]
        )
        bookInventory_df = self.map_vals_to_ids(
            bookInventory_df, "Author", bookAuthors_df, "BookAuthorKey", "BookAuthorRef"
        )

        
        bookInventory_df.rename(
            columns={
                "Id": "BookId",
                "Genre": "GenreKey",
                "Title": "BookTitleKey",
                "Author": "BookAuthorKey",
            },
            inplace=True,
        )

        
        bookInventory_df["BookCopyKey"] = list(range(1, 1 + len(bookInventory_df)))

        
        data = {
            "BookCopyKey": bookInventory_df["BookCopyKey"],
            "PurchasePrice£": bookInfo_df["PurchasePrice£"],
            "PurchaseDate": pd.to_datetime(
                bookInfo_df["PurchaseDate"], format="%d/%m/%Y"
            ).dt.strftime("%Y-%m-%d"),
        }
        bookCopies_df = pd.DataFrame(data)

        
        bookInventory_df.to_sql(
            name="BookInventory", con=self.conn, if_exists="append", index=False
        )
        genre_df.to_sql(name="Genre", con=self.conn, if_exists="append", index=False)
        bookTitles_df.to_sql(
            name="BookTitle", con=self.conn, if_exists="append", index=False
        )
        bookAuthors_df.to_sql(
            name="BookAuthor", con=self.conn, if_exists="append", index=False
        )
        bookCopies_df.to_sql(
            name="BookCopies", con=self.conn, if_exists="append", index=False
        )

        
        transactions_df = self.create_empty_transactions_fact_table(
            loanReservationHistory_df
        )

        
        for (idx, row) in loanReservationHistory_df.iterrows():
            if pd.isnull(row.loc["ReservationDate"]) == False:
                
                if pd.isnull(row.loc["CheckoutDate"]) and pd.isnull(
                    row.loc["ReturnDate"]
                ):
                    self.deactivateLastTransaction(transactions_df, row.loc["BookId"])

                    self.fill_new_fields(
                        transactions_df,
                        idx,
                        transactionTypeMsg="Reserve",
                        isCheckedOutNum=0,
                        checkedOutMemberId=np.nan,
                        isReservedNum=1,
                        reservedMemberId=row.loc["MemberId"],
                        initialDate=row.loc["ReservationDate"],
                        endRecordDate=np.nan,
                        isActive=1,
                    )
                
                elif pd.isnull(row.loc["CheckoutDate"]) == False:

                    
                    if pd.isnull(row.loc["ReturnDate"]):
                        reserve_date = datetime.strptime(
                            row.loc["ReservationDate"], "%d/%m/%Y"
                        ).strftime("%Y/%m/%d")
                        checkout_date = datetime.strptime(
                            row.loc["CheckoutDate"], "%d/%m/%Y"
                        ).strftime("%Y/%m/%d")

                        
                        if reserve_date > checkout_date:
                            last_checkedOutMemberId = (
                                self.findLastMemberId_and_deactivateLastTransaction(
                                    loanReservationHistory_df,
                                    transactions_df,
                                    row.loc["BookId"],
                                    row.loc["MemberId"],
                                )
                            )

                            self.fill_new_fields(
                                transactions_df,
                                idx,
                                transactionTypeMsg="Reserve",
                                isCheckedOutNum=1,
                                checkedOutMemberId=last_checkedOutMemberId,
                                isReservedNum=1,
                                reservedMemberId=row.loc["MemberId"],
                                initialDate=row.loc["ReservationDate"],
                                endRecordDate=np.nan,
                                isActive=1,
                            )
                        
                        else:
                            last_ReservedMemberId = (
                                self.findLastMemberId_and_deactivateLastTransaction(
                                    loanReservationHistory_df,
                                    transactions_df,
                                    row.loc["BookId"],
                                    row.loc["MemberId"],
                                )
                            )

                            self.fill_new_fields(
                                transactions_df,
                                idx,
                                transactionTypeMsg="Checkout",
                                isCheckedOutNum=1,
                                checkedOutMemberId=row.loc["MemberId"],
                                isReservedNum=1,
                                reservedMemberId=last_ReservedMemberId,
                                initialDate=row.loc["CheckoutDate"],
                                endRecordDate=np.nan,
                                isActive=1,
                            )
                    
                    else:
                        last_checkedOutMemberId = (
                            self.findLastMemberId_and_deactivateLastTransaction(
                                loanReservationHistory_df,
                                transactions_df,
                                row.loc["BookId"],
                                row.loc["MemberId"],
                            )
                        )

                        reserve_date = datetime.strptime(
                            row.loc["ReservationDate"], "%d/%m/%Y"
                        ).strftime("%Y/%m/%d")
                        return_date = datetime.strptime(
                            row.loc["ReturnDate"], "%d/%m/%Y"
                        ).strftime("%Y/%m/%d")

                        
                        if reserve_date > return_date:
                            self.fill_new_fields(
                                transactions_df,
                                idx,
                                transactionTypeMsg="Reserve",
                                isCheckedOutNum=1,
                                checkedOutMemberId=last_checkedOutMemberId,
                                isReservedNum=1,
                                reservedMemberId=row.loc["MemberId"],
                                initialDate=row.loc["ReservationDate"],
                                endRecordDate=np.nan,
                                isActive=1,
                            )
                        
                        else:
                            self.fill_new_fields(
                                transactions_df,
                                idx,
                                transactionTypeMsg="Return",
                                isCheckedOutNum=1,
                                checkedOutMemberId=last_checkedOutMemberId,
                                isReservedNum=1,
                                reservedMemberId=row.loc["MemberId"],
                                initialDate=row.loc["ReturnDate"],
                                endRecordDate=np.nan,
                                isActive=1,
                            )
            elif (
                pd.isnull(row.loc["ReservationDate"])
                and pd.isnull(row.loc["CheckoutDate"]) == False
            ):
                self.deactivateLastTransaction(transactions_df, row.loc["BookId"])

                
                if pd.isnull(row.loc["ReturnDate"]):
                    self.fill_new_fields(
                        transactions_df,
                        idx,
                        transactionTypeMsg="Checkout",
                        isCheckedOutNum=1,
                        checkedOutMemberId=row.loc["MemberId"],
                        isReservedNum=0,
                        reservedMemberId=np.nan,
                        initialDate=row.loc["CheckoutDate"],
                        endRecordDate=np.nan,
                        isActive=1,
                    )
                
                else:
                    self.fill_new_fields(
                        transactions_df,
                        idx,
                        transactionTypeMsg="Return",
                        isCheckedOutNum=1,
                        checkedOutMemberId=row.loc["MemberId"],
                        isReservedNum=0,
                        reservedMemberId=np.nan,
                        initialDate=row.loc["ReturnDate"],
                        endRecordDate=np.nan,
                        isActive=1,
                    )

        transactions_df.to_sql(
            name="Transactions", con=self.conn, if_exists="append", index=False
        )
