import pandas as pd
import numpy as np
from tkinter import messagebox
from datetime import datetime


def get_transactions_table_data(conn):
    df = pd.read_sql_query("SELECT * FROM Transactions;", conn)

    return df


class ReserveBook:
    def __init__(self, databaseObj):
        self.databaseObj = databaseObj
        self.conn = self.databaseObj.conn

        self.transactions_df = get_transactions_table_data(self.conn)

        self.new_df = self.databaseObj.create_empty_transactions_fact_table(
            self.transactions_df
        )

    def reserve_book(self, bookID, memberID):

        self.transactions_df = get_transactions_table_data(self.conn)

        
        
        last_activatedTransaction = self.databaseObj.deactivateLastTransaction(
            self.transactions_df, bookID, get_df_of_same_bookIds_via_gui=True
        )

        
        
        if (
            last_activatedTransaction is None
            or last_activatedTransaction["TransactionType"] == "Return"
        ):
            messagebox.showinfo(
                "Success",
                f"The book with an ID of {bookID} has successfully been reserved to {memberID} ID member",
            )

            
            self.databaseObj.deactivateLastTransaction(
                self.transactions_df, bookID, get_df_of_same_bookIds_via_gui=False
            )

            self.new_df = self.databaseObj.fill_new_fields(
                self.transactions_df,
                self.transactions_df.shape[0] + 1,  
                transactionTypeMsg="Reserve",
                isCheckedOutNum=0,
                checkedOutMemberId=np.nan,
                isReservedNum=1,
                reservedMemberId=memberID,
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

        
        elif last_activatedTransaction["TransactionType"] == "Reserve":

            if last_activatedTransaction["ReservedMemberId"] == memberID:
                messagebox.showerror(
                    "Error",
                    f"The book with an ID of {bookID} is already reserved by this member",
                )
            else:
                messagebox.showerror(
                    "Error",
                    f"The book with an ID of {bookID} is already reserved to the {int(last_activatedTransaction.loc['ReservedMemberId'])} ID member",
                )

        
        elif last_activatedTransaction["TransactionType"] == "Checkout":
            
            

            messagebox.showinfo(
                "Success",
                f"""The book with an ID of {bookID} has successfully been reserved to {memberID} ID member as it is already checked out by
            {int(last_activatedTransaction.loc['CheckedOutMemberId'])} ID member""",
            )

            
            self.databaseObj.deactivateLastTransaction(
                self.transactions_df, bookID, get_df_of_same_bookIds_via_gui=False
            )

            last_checkedOutMemberId = (
                self.databaseObj.findLastMemberId_and_deactivateLastTransaction(
                    self.transactions_df,
                    self.new_df,
                    bookID,
                    memberID,
                    columnOfmemberId="CheckedOutMemberId",
                )
            )

            self.new_df = self.databaseObj.fill_new_fields(
                self.transactions_df,
                self.transactions_df.shape[0] + 1,  
                transactionTypeMsg="Reserve",
                isCheckedOutNum=1,
                checkedOutMemberId=last_checkedOutMemberId,
                isReservedNum=1,
                reservedMemberId=bookID,
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
