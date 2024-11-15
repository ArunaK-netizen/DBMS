import numpy as np
import pandas as pd
from tkinter import messagebox
from datetime import datetime


def get_transactions_table_data(conn):
    df = pd.read_sql_query("SELECT * FROM Transactions;", conn)

    return df


class ReturnBook:
    def __init__(self, databaseObj):
        self.databaseObj = databaseObj
        self.conn = self.databaseObj.conn

        self.transactions_df = get_transactions_table_data(self.conn)

        self.new_df = self.databaseObj.create_empty_transactions_fact_table(
            self.transactions_df
        )

    def return_book(self, bookID, memberID):

        self.transactions_df = get_transactions_table_data(self.conn)

        
        
        last_activatedTransaction = self.databaseObj.deactivateLastTransaction(
            self.transactions_df, bookID, get_df_of_same_bookIds_via_gui=True
        )

        
        if (
            last_activatedTransaction is None
            or last_activatedTransaction["TransactionType"] == "Reserve"
        ):
            messagebox.showerror(
                "Error",
                f"The book with an ID of {bookID} cannot be returned as it has never been checked out",
            )

        elif last_activatedTransaction["TransactionType"] == "Checkout":

            
            if last_activatedTransaction["CheckedOutMemberId"] == memberID:
                messagebox.showinfo(
                    "Success",
                    f"The book with an ID of {bookID} has successfully been returned",
                )

                
                self.databaseObj.deactivateLastTransaction(
                    self.transactions_df, bookID, get_df_of_same_bookIds_via_gui=False
                )

                self.new_df = self.databaseObj.fill_new_fields(
                    self.transactions_df,
                    self.transactions_df.shape[0] + 1,  
                    transactionTypeMsg="Return",
                    isCheckedOutNum=1,
                    checkedOutMemberId=memberID,
                    isReservedNum=0,
                    reservedMemberId=np.nan,
                    initialDate=datetime.today().strftime("%d/%m/%Y"),
                    endRecordDate=np.nan,
                    isActive=1,
                    gui_flag=True,
                    bookId=bookID,
                    get_df=True,
                )

                self.new_df.tail(1).to_sql(
                    "Transactions", self.conn, if_exists="append", index=False
                )

                
                print(self.new_df.tail(1))

            else:
                messagebox.showerror(
                    "Error",
                    f"The book with an ID of {bookID} cannot be returned as the member who checked it out last was {int(last_activatedTransaction.loc['CheckedOutMemberId'])}",
                )

        elif last_activatedTransaction["TransactionType"] == "Return":

            
            if last_activatedTransaction["CheckedOutMemberId"] == memberID:
                messagebox.showerror(
                    "Error",
                    f"The book with an ID of {bookID} cannot be returned as this specific member has returned the book aready",
                )
            else:
                messagebox.showerror(
                    "Error",
                    f"The book with an ID of {bookID} cannot be returned as the member who checked it out last was {int(last_activatedTransaction.loc['CheckedOutMemberId'])}",
                )
