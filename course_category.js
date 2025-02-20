import mysql from "mysql2/promise";
import { MongoClient, ObjectId, UUID } from "mongodb";
import dotenv from "dotenv";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_COURSE,
    MONGO_URL,
    MONGO_DATABASE_COURSE,
    BATCH_SIZE,
    NEW_SITE_ID,
    OLD_SITE_ID,
} = process.env;

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_COURSE,
    });
}

async function CountRecord(tableName) {
    const query = `SELECT Count(*) FROM ${tableName} WHERE IdSite=${parseInt(OLD_SITE_ID)}`;
    const [rows] = await sqlConnection.execute(query);
    console.log(rows[0]?.['Count(*)']);

    return rows[0]?.['Count(*)']

}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${parseInt(OLD_SITE_ID)} LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function migrateTable(sqlConnection, mongoDb, tableName) {
    const countRecord = await CountRecord(tableName)

    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);



    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, countRecord);

        const mappedNews = rows.map((row) => {
            const id = new ObjectId();
            console.log({ id });
            return {
                _id: id,
                courseCatalogName: row.Name,
                courseCatalogFather: row.IdParent,
                description: row.Description,
                isHidden: row.DisplayOrder,
                courseCatalogLevel: row.IdParent ? 2 : 1,
                createdAt: row.CreatedAt,
                updatedAt: row.ModifiedAt,
                oldId: row.Id,
                siteId: +NEW_SITE_ID,
            }

        });

        const resultCata = mappedNews.map((item) => {
            if (item.courseCatalogFather) {
                const IdParent = mappedNews.find(cata => item.courseCatalogFather == cata.oldId)?._id;
                return { ...item, courseCatalogFather: IdParent }
            } else {
                return item
            }
        })

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('courseCatalog').insertMany(resultCata);

        offset += BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_COURSE);

    try {
        await migrateTable(sqlConnection, mongoDb, 'CourseCategories');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();