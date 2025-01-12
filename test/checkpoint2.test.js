import { aoslocal } from '../src/index.js'

const PROCESS = "Meb6GwY5I9QN77F0c5Ku2GpCFxtYyG1mfJus2GWYtII"
const MODULE = "EAIJew2R7aptjpyn7TD7S7ldVW4cTpUhZCaMvcerfWc"

// const PROCESS = "6poPdECzioaWeCSCf1YnZ9lkavQqaCmr3xywOWSEtm8"
// const MODULE = "pvXvNCa-svBhc1ovojvqFn3YlWiP2fZiWR7gKvEGOPQ"

async function main() {
  const aos = await aoslocal(MODULE)

  await aos.load(PROCESS)

  const Env = {
    Process: {
      Id: "Meb6GwY5I9QN77F0c5Ku2GpCFxtYyG1mfJus2GWYtII",
      Owner: "LjFZGDae9yM-yOj0Ei7ex0xy3Zdrbn8jo-7ZqVLT19E",
      Tags: [
        { name: "Data-Protocol", value: "ao" },
        { name: "Variant", value: "ao.TN.1" },
        { name: "Type", value: "Process" },
        { name: "Authority", value: "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY" }
      ],
    },
    Module: {
      Id: "EAIJew2R7aptjpyn7TD7S7ldVW4cTpUhZCaMvcerfWc",
      Tags: [
        { name: "Data-Protocol", value: "ao" },
        { name: "Variant", value: "ao.TN.1" },
        { name: "Type", value: "Module" },
      ]
    }
  }
  const result = await aos.eval("require('db.utils').rawQuery('select * from amm_transactions LIMIT 10000;')", Env)
  // const result = await aos.send({
  //   Target: "1OEAToQGhSKV76oa1MFIGZ9bYxCJoxpXqtksApDdcu8",
  //   Owner: "bUPyN5S1oR44mG1AQ51qgSZPmv985RiMqFiB3q9tUZU",
  //   Action: "Eval",
  //   Module: "L0R0-HrGcs8az_toOi06jLcBbjU0UsudpqIv9K-jBCw",
  //   Data: "Balances['vh-NTHVvlKZqRxc8LyyTNok65yQ55a_PJ1zWLb9G2JI']"
  // }, Env).catch(err => {

  //   console.log(err)
  //   return { Output: { data: '1234' } }
  // })
  //console.log(result)
  console.log(result)
  console.log(result.Output.data)


}

main()
