import mysql from "mysql2/promise";
import { MongoClient, ObjectId, UUID } from "mongodb";
import dotenv from "dotenv";
import moment from "moment";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_AUTH,
    MONGO_URL,
    MONGO_DATABASE_USER,
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
        database: MYSQL_DATABASE_AUTH,
    });
}

async function CountRecord(tableName) {
    const query = `SELECT Count(*) FROM ${tableName} WHERE IdSite=${parseInt(OLD_SITE_ID)}`;
    const [rows] = await sqlConnection.execute(query);
    console.log(rows[0]?.['Count(*)']);

    return rows[0]?.['Count(*)']

}


async function findAllUserGroup(idGroup, sqlConnection, db) {
    const query = `SELECT * FROM UsersInUserGroups WHERE idGroup=${idGroup}`;
    const [rows] = await sqlConnection.execute(query);

    const ids = rows.map(item => item.IdUser);



    const usersInGroup = await db.collection("user").find({ mobieduUserId: { $in: ids } }).toArray();

    const usersInGroupIds = usersInGroup.map(item => item._id);

    return usersInGroupIds.map(item => item._id)
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

        console.log({ rows });


        const resultCata = [];
        for (const row of rows) {

            const userInGroup = await findAllUserGroup(row.Id, sqlConnection, mongoDb)

            const id = new ObjectId();
            resultCata.push({
                _id: id,
                name: row.Name,
                numberUser: userInGroup.length,
                idUsers: userInGroup,
                oldId: row.Id,
                idManagementLevel: "",
                siteId: +NEW_SITE_ID,
                createdAt: moment(row.CreateAt).unix(),
            });
        }


        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('groupUser').insertMany(resultCata);

        offset += +BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_USER);

    try {
        await migrateTable(sqlConnection, mongoDb, 'UserGroups');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();