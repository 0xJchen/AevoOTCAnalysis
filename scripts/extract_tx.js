import {promises as fs} from "fs";
import { Alchemy, Network } from "alchemy-sdk";
import { writeFile } from "fs/promises";
import dotenv from 'dotenv';

dotenv.config();
const alchemyApiKey = process.env.API_KEY;

// ShortOtokenMinted (index_topic_1 address otoken, index_topic_2 address AccountOwner, \
//   index_topic_3 address to, uint256 vaultId, uint256 amount)

const readFileAsync = async () => {
  const data = await fs.readFile('data/tokenMintEvents.json', 'utf8');
  const arrayData = JSON.parse(data);
  const secondTopics = arrayData.result.map(item => item.topics[1]);
  const fourthTopics = arrayData.result.map(item => item.topics[3]);

  const transactionHash = arrayData.result.map(item => item.transactionHash);

  let tokenAddr = secondTopics.map(item => {
      let postfix=item.slice(-40);
      return '0x' + postfix;
  });

  let optionBuyerAddr = fourthTopics.map(item => {  
    let postfix=item.slice(-40);
    return '0x' + postfix;
  });

//   console.log(tokenAddr);
  return {tokenAddr: tokenAddr, transactionHash: transactionHash, optionBuyerAddr: optionBuyerAddr};
};

const writeToFile = async (data, filename) => {
    try {
        await writeFile(filename, JSON.stringify(data, null, 2));
        console.log(`Successfully wrote data to ${filename}`);
    } catch (error) {
        console.error(`Error writing data to file: ${error}`);
    }
};


// assuming alchemy is an instance of the alchemy API
async function getBlockNumber(alchemy, transactionHash) {
  const txReceipt = await alchemy.core.getTransactionReceipt(transactionHash);
  
  return txReceipt;
}

async function getBlockTimeStamp(alchemy, blockNumber) {
  const blockReceipt = await alchemy.core.getBlock(blockNumber);

  return blockReceipt;

}


const main = async () => {

  const config = {
    apiKey: alchemyApiKey,
    network: Network.ETH_MAINNET,
  };
  const alchemy = new Alchemy(config);

  // See README.md. Free api has a limit on 2k block range, go to https://composer.alchemy.com/ and copy the content into tokenMintEvents.json yourself.

  // get all option minting events
  // const optionMintEvents=await alchemy.core.getLogs({
  //   fromBlock:"0x0",
  //   toBlock:"latest",
  //   address:"0xFED805e631aB9Ed2b94F91255DD2714157fA759d",
  //   topics: [
  //     "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
  //   ]
  // });
  // const optionMintEventsData = await Promise.all(optionMintEvents);
  // // console.log(optionMintEventsData)
  // writeToFile(optionMintEventsData, "tokenMintEvents.json");

  let {tokenAddr: optionAddr, transactionHash: transactionHash, optionBuyerAddr: optionBuyerAddr} = await readFileAsync();


  const blockNumberPromise = transactionHash.map(transactionHash => getBlockNumber(alchemy, transactionHash));
  const blockNumberMetadata = await Promise.all(blockNumberPromise);
  let blockNumber = blockNumberMetadata.map(item => item.blockNumber)
  let premiumPaid = blockNumberMetadata.map(item => parseInt(item.logs[1].data, 16)/1e6);
  let contractAmount = blockNumberMetadata.map(item => Number((BigInt("0x"+item.logs[item.logs.length-2].data.slice(-64)))/BigInt(1e8)) )

  const txTimestampsPromises = blockNumber.map(blockNumber => getBlockTimeStamp(alchemy, blockNumber));
  const txTimeStampsMetadata = await Promise.all(txTimestampsPromises);
  let txTimeStamps=txTimeStampsMetadata.map(item => item.timestamp);

  let tokenMetadatas = await Promise.all(optionAddr.map((item,index) => {
    return alchemy.core.getTokenMetadata(item).then(data => ({
      ...data,
      tokenHash: item,
      transactionHash:transactionHash[index],
      optionBuyerAddr: optionBuyerAddr[index],
      transactionTimeStamp: txTimeStamps[index],
      premiumPaid: premiumPaid[index],
      contractAmount: contractAmount[index]
    }));
  }));
  await writeToFile(tokenMetadatas, "data/tokenInfo.json");

}

main();
