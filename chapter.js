import dotenv from "dotenv";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";

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


async function findCourseBySite(db) {
    const rows = await db.collection("course").find({ siteId: +NEW_SITE_ID }).toArray();
    return rows;
}


// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit, courseOldId) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IDCourse IN (${courseOldId.map(id => parseInt(id)).join(",")})
    AND IdParent IS NULL 
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);


    const courseInMongo = await findCourseBySite(mongoDb)

    const courseOldId = courseInMongo.map(item => item.oldId)

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE, courseOldId);

        const mappedNews = rows.map((row) => {
            return {
                courseId: courseInMongo.find(item => item.oldId == row.IDCourse)?._id.toString(),
                name: row.Name,
                order: row.DisplayOrder,
                status: 1,
                totalLesson: 0,
                typeChapter: 1,
                createAt: row.CreatedDate,
                updateAt: row.ModifiedDate,
                oldId: row.Id,
            }

        });

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('chapter').insertMany(mappedNews);

        offset += +BATCH_SIZE;
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
        await migrateTable(sqlConnection, mongoDb, 'CourseLesson');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();