using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;
using uLobby;

public class MySqlStorageManager : uLobby.IStorageManager
{
	internal string connectionString;

	internal MySqlStorageManager(MySqlConnectionStringBuilder builder)
	{
		connectionString = builder.GetConnectionString(true);
	}

	public void Initialize()
	{
	}

	public bool isReadyToUse
	{
		get { return !string.IsNullOrEmpty(connectionString); }
	}
}

public class MySqlAccountOperations : MySqlOperations, uLobby.IAccountOperations
{
	private MySqlStorageManager storageManager;

	public MySqlAccountOperations(MySqlStorageManager storageManager)
	{
		this.storageManager = storageManager;
		base.Initialize();
	}

	public IEnumerator AddAccountCoroutine(uLobby.AccountRecord account, uLobby.Request<uLobby.AccountRecord> request)
	{
		MySqlParameter[] paramsArray = new MySqlParameter[4];
		paramsArray[0] = new MySqlParameter("name", account.name);
		paramsArray[1] = new MySqlParameter("password", account.passwordHash.passwordHash);
		paramsArray[2] = new MySqlParameter("salt", account.passwordHash.salt);
		paramsArray[3] = new MySqlParameter("data", account.data);
        var operation = ExecuteNonQueryAsync.BeginInvoke(storageManager.connectionString, "INSERT INTO accounts (name,password,salt,data)VALUES(@name,@password,@salt,@data)", paramsArray, null, null);
		while (!operation.IsCompleted) yield return null;
		ExecuteNonQueryAsync.EndInvoke(operation);
        operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT id FROM accounts where name = @name and password = @password and salt = @salt LIMIT 1;", paramsArray, null, null);
		while (!operation.IsCompleted) yield return null;
		var dr = ExecuteDataReaderAsync.EndInvoke(operation);
		dr.Read();
		account.id = new AccountID(dr.GetInt32("id").ToString());
		dr.Close();
		StorageLayerUtility.RequestUtility.SetResult(request, account);
	}

	public IEnumerator GetAccountCoroutine(uLobby.AccountID accountID, string accountName, bool exceptionIfInvalid, uLobby.Request<uLobby.Account> request)
	{
		MySqlDataReader dr;
		if (accountID == null)
		{
            var operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT id,name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts WHERE name = @name;", new MySqlParameter[] { new MySqlParameter("name", accountName) }, null, null);
			while (!operation.IsCompleted) yield return null;
			dr = ExecuteDataReaderAsync.EndInvoke(operation);
		}
		else
		{
            var operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT id,name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts WHERE id = @id;", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
			while (!operation.IsCompleted) yield return null;
			dr = ExecuteDataReaderAsync.EndInvoke(operation);
		}
		if (dr.HasRows)
		{
			dr.Read();
			Account account = StorageLayerUtility.CreateAccount(ReadAccountRecordFromDataReader(dr));
			StorageLayerUtility.RequestUtility.SetResult(request, account);
		}
		else
		{
			StorageLayerUtility.RequestUtility.SetResult(request, null);
			if (exceptionIfInvalid)
			{
				StorageLayerUtility.RequestUtility.ThrowException(request, StorageLayerUtility.Exceptions.CreateAccountException("Account with id " + accountID + " and name " + accountName + " does not exist."));
			}
		}
		dr.Close();
	}

	public IEnumerator GetAccountRecordCoroutine(string name, uLobby.Request<uLobby.AccountRecord> request)
	{
        var operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT id,name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts WHERE name = @name", new MySqlParameter[] { new MySqlParameter("name", name) }, null, null);
		while (!operation.IsCompleted) yield return null;
		var dr = ExecuteDataReaderAsync.EndInvoke(operation);
		if (dr.HasRows)
		{
			dr.Read();
			StorageLayerUtility.RequestUtility.SetResult(request, ReadAccountRecordFromDataReader(dr));
		}
		else
		{
			StorageLayerUtility.RequestUtility.SetResult(request, null);
		}
		dr.Close();
	}

	public IEnumerator GetAccountRecordCoroutine(uLobby.AccountID accountID, uLobby.Request<uLobby.AccountRecord> request)
	{
        var operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT id,name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts WHERE id = @id", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
		while (!operation.IsCompleted) yield return null;
		var dr = ExecuteDataReaderAsync.EndInvoke(operation);
		if (dr.HasRows)
		{
			dr.Read();
			StorageLayerUtility.RequestUtility.SetResult(request, ReadAccountRecordFromDataReader(dr));
		}
		else
		{
			StorageLayerUtility.RequestUtility.SetResult(request, null);
		}
		dr.Close();
	}

	public IEnumerator GetNewAccountIDCoroutine(uLobby.Request<uLobby.AccountID> request)
	{
		//we use the database to generate the ids.
		StorageLayerUtility.RequestUtility.SetResult(request, new AccountID("111"));//dummy account id object which we don't use.
		yield break;
	}

	public IEnumerator UpdateAccountCoroutine(uLobby.IAccount account, uLobby.AccountUpdate update, uLobby.Request<uLobby.Account> request)
	{
		Request<AccountRecord> getAccountRequest;
		getAccountRequest = StorageLayerUtility.GetAccountRecord(account.id);
		yield return getAccountRequest.WaitUntilDone(); if (StorageLayerUtility.RequestUtility.PropagateException(getAccountRequest, request)) yield break;
		AccountRecord record = getAccountRequest.result;
		if (StorageLayerUtility.AccountUpdateUtility.isPasswordChanged(update))
			record.passwordHash = SaltedPasswordHash.GenerateSaltedPasswordHash(SaltedPasswordHash.GeneratePasswordHash(StorageLayerUtility.AccountUpdateUtility.GetPassword(update) + record.name));
		if (StorageLayerUtility.AccountUpdateUtility.IsDataChanged(update))
			record.data = StorageLayerUtility.AccountUpdateUtility.GetData(update);
		MySqlParameter[] paramsArray = new MySqlParameter[4];
		paramsArray[0] = new MySqlParameter("password", record.passwordHash.passwordHash);
		paramsArray[1] = new MySqlParameter("salt", record.passwordHash.salt);
		paramsArray[2] = new MySqlParameter("data", record.data);
		paramsArray[3] = new MySqlParameter("id", int.Parse(record.id.value));
        var operation = ExecuteNonQueryAsync.BeginInvoke(storageManager.connectionString, "UPDATE accounts SET password=@password,salt=@salt,data=@data WHERE id = @id", paramsArray, null, null);
		while (!operation.IsCompleted) yield return null;
		Account updatedAccount = StorageLayerUtility.CreateAccount(record);
		StorageLayerUtility.RequestUtility.SetResult(request, updatedAccount);
	}
}

public class MySqlFriendOperations : MySqlOperations, uLobby.IFriendOperations
{
	private MySqlStorageManager storageManager;

	public MySqlFriendOperations(MySqlStorageManager storageManager)
	{
		this.storageManager = storageManager;
		base.Initialize();
	}

	public IEnumerator GetFriendListRecordCoroutine(uLobby.AccountID accountID, uLobby.Request<uLobby.FriendListRecord> request)
	{
		FriendListRecord friendList = StorageLayerUtility.FriendListRecordUtility.CreateFriendListRecord();
		//read friend list
        var operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT accounts.id,accounts.name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts INNER JOIN friends ON accounts.id = friends.friend WHERE owner = @id", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
		while (!operation.IsCompleted) yield return null;
		var dr = ExecuteDataReaderAsync.EndInvoke(operation);
		while (dr.Read())
		{
			AccountRecord accountRecord = ReadAccountRecordFromDataReader(dr);
			Account account = StorageLayerUtility.CreateAccount(accountRecord);
			StorageLayerUtility.FriendListRecordUtility.AddFriend(friendList,StorageLayerUtility.CreateFriendInfo(account));
		}
		dr.Close();
		//read the original account owning the list.
        operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT id,name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts WHERE id = @id", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
		while (!operation.IsCompleted) yield return null;
		dr = ExecuteDataReaderAsync.EndInvoke(operation);
		dr.Read();
		Account originalAccount = StorageLayerUtility.CreateAccount(ReadAccountRecordFromDataReader(dr));
		dr.Close();

		//read invitations
        operation = ExecuteDataReaderAsync.BeginInvoke(storageManager.connectionString, "SELECT accounts.id,name,password,LENGTH(password),salt,LENGTH(salt),data,LENGTH(data) FROM accounts INNER JOIN invitations ON accounts.id = invitations.sender WHERE invitations.receiver = @id", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
		while (!operation.IsCompleted) yield return null;
		dr = ExecuteDataReaderAsync.EndInvoke(operation);
		while (dr.Read())
		{
			Account account = StorageLayerUtility.CreateAccount(ReadAccountRecordFromDataReader(dr));
			StorageLayerUtility.FriendListRecordUtility.AddFriendInvitation(friendList,StorageLayerUtility.CreateFriendInvitation(account, originalAccount));
		}
		dr.Close();
		StorageLayerUtility.RequestUtility.SetResult(request, friendList);
		yield break;
	}

	public IEnumerator SetFriendListRecordCoroutine(uLobby.AccountID accountID, uLobby.FriendListRecord record, uLobby.Request request)
	{
        var operation = ExecuteNonQueryAsync.BeginInvoke(storageManager.connectionString, "DELETE FROM friends WHERE owner = @id", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
		while (!operation.IsCompleted) yield return null;
		ExecuteNonQueryAsync.EndInvoke(operation);
		operation = ExecuteNonQueryAsync.BeginInvoke(storageManager.connectionString, "DELETE FROM invitations WHERE sender = @id", new MySqlParameter[] { new MySqlParameter("id", int.Parse(accountID.value)) }, null, null);
		while (!operation.IsCompleted) yield return null;
		ExecuteNonQueryAsync.EndInvoke(operation);
		foreach (AccountID friend in StorageLayerUtility.FriendListRecordUtility.GetFriendIDs(record))
		{
			MySqlParameter[] paramsArray = new MySqlParameter[2];
			paramsArray[0] = new MySqlParameter("owner", int.Parse(accountID.value));
			paramsArray[1] = new MySqlParameter("friend", int.Parse(friend.value));
            operation = ExecuteNonQueryAsync.BeginInvoke(storageManager.connectionString, "INSERT INTO friends (owner,friend) VALUES(@owner,@friend)", paramsArray, null, null);
			while (!operation.IsCompleted) yield return null;
			ExecuteNonQueryAsync.EndInvoke(operation);
		}
		foreach (var inviter in StorageLayerUtility.FriendListRecordUtility.GetInviterIDs(record))
		{
			MySqlParameter[] paramsArray = new MySqlParameter[2];
			paramsArray[0] = new MySqlParameter("sender", int.Parse(inviter.value));
			paramsArray[1] = new MySqlParameter("receiver", int.Parse(accountID.value));
			operation = ExecuteNonQueryAsync.BeginInvoke(storageManager.connectionString, "INSERT INTO invitations (sender,receiver) VALUES(@sender,@receiver)", paramsArray,null,null);
			while (!operation.IsCompleted) yield return null;
			ExecuteNonQueryAsync.EndInvoke(operation);
		}
		yield break;
	}
}

/// <summary>
/// The classes which need to do mysql account operations will inherit from this to have access to the common functionality
/// provided here.
/// </summary>
public class MySqlOperations
{
	//We define some delegates to put our methods for executing queries in them and be able to use the unity supported
	//BeginInvoke and EndInvoke async pattern for easy to use and understand multi threading.
	protected delegate void ExecuteNonQueryCallback(string connectionString, string query, MySqlParameter[] paramsArray);
	protected delegate MySqlDataReader ExecuteDataReaderCallback(string connectionString, string query, MySqlParameter[] paramsArray);
	protected ExecuteNonQueryCallback ExecuteNonQueryAsync;
	protected ExecuteDataReaderCallback ExecuteDataReaderAsync;

	protected void Initialize()
	{
		ExecuteNonQueryAsync = (s, q, p) => MySqlHelper.ExecuteNonQuery(s, q, p);
		ExecuteDataReaderAsync = (s, q, p) => MySqlHelper.ExecuteReader(s, q, p);
	}


	/// <summary>
	/// This method gets a data reader which it's Read method is called and assumes the following structure
	/// (name,password,password length,salt,salt length,data,data length)
	/// </summary>
	/// <param name="dr"></param>
	/// <returns></returns>
	protected AccountRecord ReadAccountRecordFromDataReader(MySqlDataReader dr)
	{
		int passwordLength = dr.GetInt32(3);
		int saltLength = dr.GetInt32(5);
		int dataLength = dr.GetInt32(7);
		byte[] passwordBuffer = new byte[passwordLength];
		byte[] saltBuffer = new byte[saltLength];
		byte[] dataBuffer = (dataLength != 0) ? new byte[dataLength] : null;

		if (dataBuffer != null)
			dr.GetBytes(dr.GetOrdinal("data"), 0, dataBuffer, 0, dataBuffer.Length);
		dr.GetBytes(dr.GetOrdinal("password"), 0, passwordBuffer, 0, passwordBuffer.Length);
		dr.GetBytes(dr.GetOrdinal("salt"), 0, saltBuffer, 0, saltBuffer.Length);
		AccountRecord record = StorageLayerUtility.CreateAccountRecord(dr.GetString("name"),
			new uLobby.SaltedPasswordHash(passwordBuffer, saltBuffer),
			new uLobby.AccountID(dr.GetInt32("id").ToString()),
			dataBuffer);
		return record;
	}
}