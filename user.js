import dotenv from "dotenv";
import { MongoClient, ObjectId } from "mongodb";
import mysql from "mysql2/promise";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_AUTH,
    MONGO_URL,
    MONGO_DATABASE_LEVEL,
    MONGO_DATABASE_ADMIN,
    MONGO_DATABASE_USER,
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
        database: MYSQL_DATABASE_AUTH,
    });
}

function getRoleName(IdType) {
    if (IdType == 2) return "ADMIN"
    if (IdType == 3) return "TEACHER"
    if (IdType == 4) return "STUDENT"
    return ""
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${OLD_SITE_ID} AND IsDeleted=0 LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}


async function fetchManagerLevel(sqlConnection, IdUser) {
    const query = `SELECT * FROM UsersInUnitManagements WHERE IdUser=${IdUser}`;
    const [rows] = await sqlConnection.execute(query);
    return rows;
}


async function fetchJobPosition(sqlConnection, IdUser) {
    const query = `SELECT * FROM UsersJobPosition WHERE IdUser=${IdUser}`;
    const [rows] = await sqlConnection.execute(query);

    return rows?.[0] || null;
}

async function findAllPosition(mongoDbAdmin) {
    const rows = await mongoDbAdmin.collection("position").find({ siteId: + NEW_SITE_ID }).toArray();
    return rows;
}

async function findManagerLevel(mongoDbLevel, level) {
    const rows = await mongoDbLevel.collection(level).find({ siteId: + NEW_SITE_ID }).toArray();
    return rows;
}

async function deleteOldUsers(db) {
    await db.collection('user').deleteMany({
        siteId: +NEW_SITE_ID,
        mobieduUserId: { $ne: null }  // Ensures oldId is not null
    });
}

async function deleteOldTeacher(dbCourse) {
    await dbCourse.collection('teachers').deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null },  // Ensures oldId is not null
        fromMig: "USER"
    });
}


async function CreateOrFindTeacher(
    teacherInDb,
    mongoDbCourse,
) {
    let teacher = await mongoDbCourse
        .collection("teachers")
        .findOne({ oldId: teacherInDb.Id, siteId: +NEW_SITE_ID });

    if (teacher) return teacher._id.toString()

    if (!teacher) {
        const newTeacher = {
            _id: new ObjectId(),
            avatar: teacherInDb.AvatarUrl,
            fullName: teacherInDb.FullName,
            email: teacherInDb.Email,
            phone: teacherInDb.Phone,
            personal: "",
            linkYoutube: "",
            linkFb: "",
            description: teacherInDb.Description,
            siteId: +NEW_SITE_ID,
            oldId: teacherInDb.Id,
            createdAt: new Date(),
            fromMig: "USER"
        };

        await mongoDbCourse
            .collection("teachers")
            .insertOne(newTeacher);

        return newTeacher._id.toString();
    }
}

async function migrateTable(sqlConnection, mongoDb, tableName, mongoDbLevel, mongoDbCourse, mongoDbAdmin) {
    console.log(`🔄 Đang xóa user MIGRATE cũ...`);
    await deleteOldUsers(mongoDb)
    await deleteOldTeacher(mongoDbCourse)

    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    const managerLevel1 = await findManagerLevel(mongoDbLevel, 'level-1');
    const managerLevel2 = await findManagerLevel(mongoDbLevel, 'level-2');
    const managerLevel3 = await findManagerLevel(mongoDbLevel, 'level-3');

    const listPositon = await findAllPosition(mongoDbAdmin)

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);
        const mappedUsers = [];

        for (const row of rows) {

            const managerLevelRec = await fetchManagerLevel(sqlConnection, row.Id)
            const managerLevel = managerLevelRec.map(item => item.IdUnit);
            const jobPosition = await fetchJobPosition(sqlConnection, row.Id)

            const level1 = managerLevel1.find(item => managerLevel.includes(item.oldId))
            const level2 = managerLevel2.find(item => managerLevel.includes(item.oldId))
            const level3 = managerLevel3.find(item => managerLevel.includes(item.oldId))

            const role = getRoleName(row.IdType)

            let IDTeacher = ""
            if (role == "TEACHER") {
                IDTeacher = await CreateOrFindTeacher(row, mongoDbCourse);
            }

            const positionMongo = !!jobPosition ? listPositon.find(item => item.oldId == jobPosition.IdPosition) : null

            mappedUsers.push({
                accessFailedCount: 0,
                address: "",
                avatar: row.AvatarUrl, //? row.AvatarUrl.replace("https://cdn4t.mobiedu.vn", "https://media-moocs.mobifone.vn") : ""
                birthday: row.Birthday,
                dateLogin: row.LastLoginDate,
                email: row.Email || "",
                fullname: row.FullName,
                gender: row.IdGender || 0,
                exploreField: {},
                siteId: +NEW_SITE_ID,
                positionId: !!positionMongo ? positionMongo?._id.toString() : -1,
                infoManagementLevel: null,
                infoManagementLevel: {
                    managerLevel1Id: level3?.level1.toString() || level2?.level1.toString() || level1?._id.toString() || "",
                    managerLevel2Id: level3?.level2.toString() || level2?._id.toString() || "",
                    managerLevel3Id: level3?._id.toString() || "",
                },
                isLockoutEnabled: row.IsBlocked == 0 ? true : false,
                lockoutEndDate: null,
                passwordHash: "AQAAAAIAAYagAAAAEF/xjjPGTzQb6vV7LwZowfmvbHEmHiqnFpqbHy79JmfLsCbZhUqpwdPOdMt77sameg==",
                phone: row.Phone || "",
                pwd: "Cantho@123",
                securityStamp: "KX25LUUYU6RI4T7H3IY5MINGRQDEZ4UH",
                status: 1,
                timeUpdate: null,
                userName: row.Email,
                mobieduUserId: row.Id,
                listRoles: {},
                listPolicy: [],
                role: role,
                functionsTree: {},
                idTeacher: IDTeacher,
            });
        }

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) {
            break;
        }

        await mongoDb.collection('user').insertMany(mappedUsers);

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
    const mongoDbLevel = mongoClient.db(MONGO_DATABASE_LEVEL);
    const mongoDbCourse = mongoClient.db(MONGO_DATABASE_COURSE);

    const mongoDbAdmin = mongoClient.db(MONGO_DATABASE_ADMIN);


    try {
        await migrateTable(sqlConnection, mongoDb, 'Users', mongoDbLevel, mongoDbCourse, mongoDbAdmin);
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();
