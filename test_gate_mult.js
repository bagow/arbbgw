async function run() {
    const res = await fetch("https://api.gateio.ws/api/v4/futures/usdt/contracts");
    const json = await res.json();
    for (const c of json) {
        if (c.name.includes("TAG")) {
            console.log(c.name, "Multiplier:", c.quanto_multiplier);
        }
        if (c.name.includes("LAB")) {
            console.log(c.name, "Multiplier:", c.quanto_multiplier);
        }
    }
}
run();
