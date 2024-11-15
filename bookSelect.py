from treeView import TreeViewClass


class BookRecommendationSystem:
    def __init__(self, databaseObj):
        self.databaseObj = databaseObj
        self.conn = self.databaseObj.conn

    def most_rcr_per_copy(self, tree, transactionTypeSubitted):
        query = f"""SELECT t.BookId, bt.BookTitleRef, COUNT(t.BookId)
   FROM Transactions t
   INNER JOIN BookInventory bi on bi.BookId = t.BookId
   INNER JOIN BookTitle bt on bt.BookTitleKey = bi.BookTitleKey
   WHERE TransactionType = '{transactionTypeSubitted}'
   GROUP BY t.BookId
   LIMIT 5;"""

        TreeViewClass(self.databaseObj, tree, query)

    def most_rcr_per_book(self, tree, transactionTypeSubitted):
        query = f"""SELECT bt.BookTitleRef, COUNT(bi.BookTitleKey)
   FROM BookInventory bi
   INNER JOIN BookTitle bt on bt.BookTitleKey = bi.BookTitleKey
   INNER JOIN Transactions t on t.BookId = bi.BookId
   WHERE t.TransactionType = '{transactionTypeSubitted}'
   GROUP BY bi.BookTitleKey
   LIMIT 5;"""

        TreeViewClass(self.databaseObj, tree, query)

    def most_rcr_available_days(self, tree, transactionTypeSubitted):
        query = f"""SELECT bt.BookTitleRef, COUNT(bi.BookTitleKey), CAST(SUM(JULIANDAY('now') - JULIANDAY(bc.PurchaseDate)) AS INTEGER) AS Available_days
   FROM BookCopies bc
   INNER JOIN BookInventory bi on bi.BookCopyKey = bc.BookCopyKey
   INNER JOIN Transactions t on t.BookId = bi.BookId
   INNER JOIN BookTitle bt on bt.BookTitleKey = bi.BookTitleKey
   WHERE t.TransactionType = '{transactionTypeSubitted}'
   GROUP BY bi.BookTitleKey
   LIMIT 5;"""

        TreeViewClass(
            self.databaseObj,
            tree,
            query,
            book_search_flag=False,
            available_days_flag=True,
        )

    def most_rcr_per_genre(self, tree, transactionTypeSubitted):
        query = f"""SELECT g.GenreRef, COUNT(bi.GenreKey)
   FROM BookInventory bi
   INNER JOIN Genre g on g.GenreKey = bi.GenreKey
   INNER JOIN Transactions t on t.BookId = bi.BookId
   WHERE t.TransactionType = '{transactionTypeSubitted}'
   GROUP BY bi.GenreKey
   LIMIT 5;"""

        TreeViewClass(self.databaseObj, tree, query)
