package shardmaster

import "sort"

type ConfigStateMachine interface {
	Join(servers map[int][]string) Err
	Leave(gids []int) Err
	Move(shard int, gid int) Err
	Query(num int) (Config, Err)
}

type MemConfig struct {
	configs []Config
}

// NewMemConfig must return pointer
func NewMemConfig() *MemConfig {
	return &MemConfig{make([]Config, 1)}
}

func (mc *MemConfig) Join(groups map[int][]string) Err {

	latest := mc.configs[len(mc.configs)-1]
	newConfig := Config{
		latest.Num + 1,
		latest.Shards,
		copyMap(latest.Groups),
	}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	g2s := group2Shards(newConfig)
	DPrintf("g2s: %v", g2s)
	for {
		free, busy := getFreeGroup(g2s), getBusyGroup(g2s)
		// busy == 0 means there are some shards haven't been assigned to valid groups,
		// so don't break until no shard's group is zero
		if busy != 0 && len(g2s[busy])-len(g2s[free]) <= 1 {
			break
		}
		g2s[free] = append(g2s[free], g2s[busy][0]) // increase free group's load
		g2s[busy] = g2s[busy][1:]                   // decrease busy group's load
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	mc.configs = append(mc.configs, newConfig)

	return OK
}

func (mc *MemConfig) Leave(gids []int) Err {

	latest := mc.configs[len(mc.configs)-1]
	newConfig := Config{
		latest.Num + 1,
		latest.Shards,
		copyMap(latest.Groups),
	}
	g2s := group2Shards(newConfig)

	freeShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			freeShards = append(freeShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range freeShards {
			free := getFreeGroup(g2s)
			g2s[free] = append(g2s[free], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	mc.configs = append(mc.configs, newConfig)
	return OK
}

func (mc *MemConfig) Move(shard int, gid int) Err {
	latest := mc.configs[len(mc.configs)-1]

	shards := latest.Shards
	shards[shard] = gid

	c := Config{
		latest.Num + 1,
		shards,
		copyMap(latest.Groups),
	}

	mc.configs = append(mc.configs, c)
	return OK
}

func (mc *MemConfig) Query(num int) (Config, Err) {
	if num >= len(mc.configs) || num < 0 {
		return mc.configs[len(mc.configs)-1], OK
	}
	c := mc.configs[num]
	return c, OK
}

func copyMap(sour map[int][]string) map[int][]string {
	dest := make(map[int][]string)
	for k, v := range sour {
		dest[k] = v
	}
	return dest
}

func group2Shards(c Config) map[int][]int {
	g2s := make(map[int][]int)

	for key, _ := range c.Groups {
		g2s[key] = make([]int, 0)
	}
	for i, shard := range c.Shards {
		g2s[shard] = append(g2s[shard], i)
	}
	return g2s
}

func getFreeGroup(g2s map[int][]int) int {
	var key []int
	for k, _ := range g2s {
		key = append(key, k)
	}
	sort.Ints(key)

	freeGroupId := -1
	min := NShards + 1
	for _, id := range key {
		if id != 0 && min > len(g2s[id]) {
			min = len(g2s[id])
			freeGroupId = id
		}
	}
	return freeGroupId
}

func getBusyGroup(g2s map[int][]int) int {
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}
	var key []int
	for k, _ := range g2s {
		key = append(key, k)
	}
	sort.Ints(key)

	busyGroupId := -1
	max := -1
	for _, id := range key {
		if max < len(g2s[id]) {
			max = len(g2s[id])
			busyGroupId = id
		}
	}
	return busyGroupId
}
