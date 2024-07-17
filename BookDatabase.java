import java.sql.*;

public class BookDatabase {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "bookBD";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            System.out.println("Вывод всех книг:");
            getAllBooks(connection); System.out.println();

            // TODO show with param
            System.out.println("Вывод книг, у которых автор с id 1");
            getBooksByAuthor(connection, 1); System.out.println();
            System.out.println("Вывод книг, у которых название Божественная Комедия");
            getBooksByTitle(connection, "Божественная Комедия"); System.out.println();
            System.out.println("Вывод книг, у которых издание с id 1");
            getBooksByPublisher(connection, 1); System.out.println();
            System.out.println("Вывод книг, у которых isbn 978-5-04-117290-9");
            getBooksByISBN(connection, "978-5-04-117290-9"); System.out.println();
            System.out.println("Вывод книг, у которых серия с id 1");
            getBooksBySeries(connection, 1); System.out.println();
            System.out.println("Вывод книг, у которых название начинается с буквы Д");
            getBooksStartsWithLetter(connection, 'Д'); System.out.println();
            System.out.println("Сортировка книг по названию");
            getBooksSortedByTitle(connection); System.out.println();
            System.out.println("Вывод книг, у которых страниц больше 400, но меньше 600");
            getBooksByPageCount(connection, 400, 600); System.out.println();

            // TODO correction
            String title = "Новая книга";
            int authorId = 1;
            int publisherId = 1;
            int pageCount = 200;
            String isbn = "978-0-00-000000-0";
            int serie_id = 1;
            addBook(connection, title, authorId, publisherId, pageCount, isbn, serie_id); System.out.println();
            deleteBook(connection, 1); System.out.println(); // Замените 1 на ID книги
            System.out.println("Вывод книг с полными именем автора, названием издательства и названием серии");
            getBooksWithAuthorName(connection); System.out.println();
            System.out.println("Вывод книг с определённым издательством и серией");
            getBooksByPublisherAndSeries(connection, 1, 1); System.out.println(); // Замените 1 на ID издательства и серии
            System.out.println("Вывод книги, у которой больше всего страниц");
            getBooksWithMostPages(connection); System.out.println();
            System.out.println("Вывод автора, у которого больше всего книг в БД");
            getAuthorsWithMostBooks(connection); System.out.println();

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // region // Проверка окружения и доступа к базе данных

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // endregion

    // region // SELECT-запросы без параметров в одной таблице

    public static void getAllBooks(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "book_id", columnName1 = "title", columnName2 = "author_id", columnName3 = "publisher_id";
        String columnName4 = "page_count", columnName5 = "isbn", columnName6 = "serie_id";
        // значения ячеек
        int param0 = -1, param2 = -1, param3 = -1, param4 = -1, param6 = -1;
        String param1 = null, param5 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM book;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param6 = rs.getInt(columnName6);
            param5 = rs.getString(columnName5);
            param4 = rs.getInt(columnName4);
            param3 = rs.getInt(columnName3);
            param2 = rs.getInt(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6);
        }
    }

    // endregion

    // region // SELECT-запросы с параметрами и объединением таблиц

    private static void getBooksByAuthor(Connection connection, int authorId) throws SQLException {
        if (authorId < 0) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book.book_id, book.title " +
                        "FROM book " +
                        "WHERE book.author_id = ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, authorId);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksByTitle(Connection connection, String title) throws SQLException {
        if (title == null || title.isBlank()) return; // проверка "на дурака"
        title = '%' + title + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book_id, title, author_id, publisher_id, page_count, isbn, serie_id " +
                        "FROM book " +
                        "WHERE title LIKE ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, title);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5) + " | " + rs.getString(6) + " | " + rs.getInt(7));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksByPublisher(Connection connection, int publisherId) throws SQLException {
        if (publisherId < 0) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book.book_id, book.title " +
                        "FROM book " +
                        "WHERE book.publisher_id = ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, publisherId);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksByISBN(Connection connection, String isbn) throws SQLException {
        if (isbn == null || isbn.isBlank()) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book_id, title, author_id, publisher_id, page_count, isbn, serie_id " +
                        "FROM book " +
                        "WHERE isbn = ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, isbn);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5) + " | " + rs.getString(6) + " | " + rs.getInt(7));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksBySeries(Connection connection, int seriesId) throws SQLException {
        if (seriesId < 0) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book.book_id, book.title " +
                        "FROM book " +
                        "WHERE book.serie_id = ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, seriesId);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksStartsWithLetter(Connection connection, char letter) throws SQLException {
        if (letter < 0 || letter > 255) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book_id, title, author_id, publisher_id, page_count, isbn, serie_id " +
                        "FROM book " +
                        "WHERE title LIKE ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, letter + "%");           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5) + " | " + rs.getString(6) + " | " + rs.getInt(7));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksSortedByTitle(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book_id, title, author_id, publisher_id, page_count, isbn, serie_id " +
                        "FROM book " +
                        "ORDER BY title;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5) + " | " + rs.getString(6) + " | " + rs.getInt(7));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksByPageCount(Connection connection, int minPages, int maxPages) throws SQLException {
        if (minPages < 0 || maxPages < 0 || minPages > maxPages) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book_id, title, author_id, publisher_id, page_count, isbn, serie_id " +
                        "FROM book " +
                        "WHERE page_count > ? AND page_count < ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, minPages);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        statement.setInt(2, maxPages);
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5) + " | " + rs.getString(6) + " | " + rs.getInt(7));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    // endregion

    // region // CUD-запросы на добавление, изменение и удаление записей

    private static void addBook(Connection connection, String title, int authorId, int publisherId, int pageCount, String isbn, int serie_id)  throws SQLException {
        if (title == null || title.isBlank() || authorId < 0 || publisherId < 0 || pageCount < 0 || isbn == null || isbn.isBlank() || serie_id < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO book(book_id, title, author_id, publisher_id, page_count, isbn, serie_id) VALUES (DEFAULT, ?, ?, ?, ?, ?, ?) returning book_id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, title);
        statement.setInt(2, authorId);
        statement.setInt(3, publisherId);
        statement.setInt(4, pageCount);
        statement.setString(5, isbn);
        statement.setInt(6, serie_id);

        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор книги " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " book");
        getAllBooks(connection);
    }

    private static void deleteBook(Connection connection, int bookId) throws SQLException {
        if (bookId < 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from book WHERE book_id=?;");
        statement.setInt(1, bookId);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " book");
        getAllBooks(connection);
    }

    // endregion

    private static void getBooksWithAuthorName(Connection connection) throws SQLException {
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book.title, author.name, publisher.publisher_name, serie.serie_name " +
                        "FROM book " +
                        "JOIN author ON book.author_id = author.id_author " +
                        "JOIN publisher ON book.publisher_id = publisher.id_publisher " +
                        "JOIN serie ON book.serie_id = serie.id_serie;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with JOIN (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksByPublisherAndSeries(Connection connection, int publisherId, int seriesId) throws SQLException {
        if (publisherId < 0 || seriesId < 0) return;

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book.book_id, book.title " +
                        "FROM book " +
                        "JOIN publisher ON book.publisher_id = publisher.id_publisher " +
                        "JOIN serie ON book.serie_id = serie.id_serie " +
                        "WHERE publisher.id_publisher = ? AND serie.id_serie = ?;");
        statement.setInt(1, publisherId);
        statement.setInt(2, seriesId);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with JOIN (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getBooksWithMostPages(Connection connection) throws SQLException {
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT book.book_id, book.title " +
                        "FROM book " +
                        "WHERE book.page_count = (SELECT MAX(page_count) FROM book) LIMIT 1;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with Subquery (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getAuthorsWithMostBooks(Connection connection) throws SQLException {
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT author.id_author, author.name " +
                        "FROM author " +
                        "JOIN book ON author.id_author = book.author_id " +
                        "GROUP BY author.id_author, author.name " +
                        "ORDER BY COUNT(book.book_id) DESC " +
                        "LIMIT 1;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with GROUP BY (" + (System.currentTimeMillis() - time) + " мс.)");
    }
}