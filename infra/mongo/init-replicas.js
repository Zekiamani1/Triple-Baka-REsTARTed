// Re-runnable bootstrap, including expansion of the earlier one-member sets.
const command = require("/scripts/command.cjs");
async function waitFor(description, operation) {
  let last;
  for (let i = 0; i < 120; i++) {
    try { return await operation(); } catch (error) { last = error; sleep(1000); }
  }
  throw new Error(`${description}: ${last}`);
}

(async () => {
  for (const spec of [
    { name: "configRS", config: true, hosts: ["mongo-config:27019", "mongo-config-2:27019", "mongo-config-3:27019"] },
    { name: "shard1RS", hosts: ["mongo-shard-1:27018", "mongo-shard-1-replica-2:27018", "mongo-shard-1-replica-3:27018"] },
    { name: "shard2RS", hosts: ["mongo-shard-2:27018", "mongo-shard-2-replica-2:27018", "mongo-shard-2-replica-3:27018"] },
  ]) {
    for (const host of spec.hosts) {
      await waitFor(host, () => {
        const admin = new Mongo(`mongodb://${host}/?directConnection=true&serverSelectionTimeoutMS=2000`).getDB("admin");
        command(admin.runCommand({ ping: 1 }));
      });
    }
    const first = new Mongo(`mongodb://${spec.hosts[0]}/?directConnection=true`).getDB("admin");
    let status;
    try {
      status = first.runCommand({ replSetGetStatus: 1 });
    } catch (error) {
      if (error && error.code === 94) {
        status = { code: 94 };
      } else {
        throw error;
      }
    }
    if (status.code === 94) {
      command(first.runCommand({ replSetInitiate: {
        _id: spec.name,
        ...(spec.config ? { configsvr: true } : {}),
        members: spec.hosts.map((host, _id) => ({ _id, host })),
      } }));
    } else {
      command(status);
      if (status.set !== spec.name) throw new Error("Unexpected replica set; refusing to replace it");
    }
    const admin = await waitFor(`${spec.name} primary`, () => {
      const result = new Mongo(`mongodb://${spec.hosts.join(",")}/?replicaSet=${spec.name}&serverSelectionTimeoutMS=3000`).getDB("admin");
      command(result.runCommand({ replSetGetConfig: 1 }));
      return result;
    });
    // Expand existing one-member configurations one member at a time.
    for (const host of spec.hosts) {
      const config = command(admin.runCommand({ replSetGetConfig: 1 })).config;
      if (!config.members.some(member => member.host === host)) {
        config.members.push({ _id: Math.max(...config.members.map(m => m._id)) + 1, host });
        config.version++;
        command(admin.runCommand({ replSetReconfig: config }));
      }
      await waitFor(`${host} replication`, () => {
        const members = command(admin.runCommand({ replSetGetStatus: 1 })).members;
        if (!members.some(m => m.name === host && [1, 2].includes(m.state)))
          throw new Error(`${host} is still synchronizing`);
      });
    }
    await waitFor(`${spec.name} ready`, () => {
      const members = command(admin.runCommand({ replSetGetStatus: 1 })).members;
      if (members.filter(m => m.state === 1).length !== 1 ||
          members.filter(m => m.state === 2).length !== 2)
        throw new Error(`${spec.name} is not ready yet`);
    });
    // Explicit cluster-wide default before registering a replica set as a shard.
    try {
      command(admin.runCommand({ setDefaultRWConcern: 1,
        defaultWriteConcern: { w: "majority", wtimeout: 10000 } }));
    } catch (error) {
      print(`${spec.name}: setDefaultRWConcern skipped (${error.message ?? error})`);
    }
    print(`${spec.name}: PRIMARY + 2 SECONDARY ready`);
  }
})().catch((err) => {
  print(err);
  quit(1);
});
