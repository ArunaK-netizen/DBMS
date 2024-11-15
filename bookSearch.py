from tkinter import messagebox
from treeView import TreeViewClass


class SearchBookTitle:
    def __init__(self, databaseObj, tree):
        self.databaseObj = databaseObj
        self.conn = self.databaseObj.conn
        self.tree = tree

    def find_books(self, text):
        
        if len(text.strip()) != 0:
            search_query = f"""SELECT bi.BookId, g.GenreRef, bt.BookTitleRef, ba.BookAuthorRef, bc.PurchasePrice£, bc.PurchaseDate
        FROM BookInventory bi
        INNER JOIN Genre g on g.GenreKey = bi.GenreKey
        INNER JOIN BookTitle bt on bt.BookTitleKey = bi.BookTitleKey
        INNER JOIN BookAuthor ba on ba.BookAuthorKey = bi.BookAuthorKey
        INNER JOIN BookCopies bc on bc.BookCopyKey = bi.BookCopyKey
        WHERE bt.BookTitleRef LIKE '%{text}%';"""

            TreeViewClass(
                self.databaseObj, self.tree, search_query, book_search_flag=True
            )
        else:
            messagebox.showerror("Error", "No empty submission is allowed")
