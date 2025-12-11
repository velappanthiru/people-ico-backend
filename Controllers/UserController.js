const dotenv = require('dotenv').config()
const ethers = require('ethers')
const BN = require('bignumber.js')
const fastify = require('fastify')({
  ajv: {
    customOptions: {
      allErrors: true, 
      keywords: ['errorMessage']
    }
  }
});

fastify.register(require('ajv-errors'));

module.exports = async function (fastify, opts) {
  const { toUtcISOString } = require('./timeUtils');
    function generateReferralId() {
        const prefix = 'REFKDO';
        const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        const remainingLength = 15 - prefix.length; 
        let randomPart = '';
        for (let i = 0; i < remainingLength; i++) {
          randomPart += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return prefix + randomPart;
      }
      
      // fastify.get('/RegisterNewUser', {
      //   schema: {
      //     querystring: {
      //       type: 'object',
      //       required: ['wallet_address'],
      //       properties: {
      //         wallet_address: { type: 'string' },
      //         ref_id: { type: 'string' }
      //       }
      //     }
      //   }
      // }, async (request, reply) => {
      //   try {
      //     const { wallet_address, ref_id } = request.query;
      
      //     const existingUsers = await fastify.mysql.query(
      //       'SELECT * FROM users WHERE wallet_address = ?',
      //       [wallet_address]
      //     );
      
      //     if (existingUsers.length > 0) {
      //       const user = existingUsers[0];
      //       console.log(user, 'user');
      
      //       if (ref_id && user.referred_by === 'ADMIN') {
      //         const refData = await fastify.mysql.query(
      //           'SELECT * FROM users WHERE PTC_REF_ID = ?',
      //           [ref_id]
      //         );
      
      //         if (refData.length === 0) {
      //           return reply.code(400).send({
      //             status: false,
      //             msg: 'Invalid referral ID'
      //           });
      //         }
      
      //         const referrer_wallet = refData[0].wallet_address;
      
      //         if (referrer_wallet.toLowerCase() === wallet_address.toLowerCase()) {
      //           return reply.code(400).send({
      //             status: false,
      //             msg: 'You cannot refer yourself'
      //           });
      //         }
      
      //         await fastify.mysql.query(
      //           'UPDATE users SET referred_by = ?, referrer_address = ? WHERE wallet_address = ?',
      //           [ref_id, referrer_wallet, wallet_address]
      //         );
      
      //         return reply.code(200).send({
      //           status: true,
      //           msg: 'User updated with referral info',
      //           userData: {
      //             ...user,
      //             referred_by: ref_id,
      //             referrer_address: referrer_wallet
      //           }
      //         });
      //       }
      
      //       return reply.code(200).send({
      //         status: true,
      //         msg: 'User already saved',
      //         userData: user
      //       });
      //     }
      
      //     let referred_by = 'ADMIN';
      //     let referrer_wallet = process.env.OWNER_WALLET ?? '0x5FcC2fA3a76599f6e672da59CBDC0a37859CD732';
      
      //     if (ref_id) {
      //       const refData = await fastify.mysql.query(
      //         'SELECT * FROM users WHERE PTC_REF_ID = ?',
      //         [ref_id]
      //       );
      
      //       if (refData.length === 0) {
      //         return reply.code(400).send({
      //           status: false,
      //           msg: 'Invalid referral ID'
      //         });
      //       }
      
      //       const fetchedReferrerWallet = refData[0].wallet_address;
      
      //       if (fetchedReferrerWallet.toLowerCase() === wallet_address.toLowerCase()) {
      //         return reply.code(400).send({
      //           status: false,
      //           msg: 'You cannot refer yourself'
      //         });
      //       }
      
      //       referrer_wallet = fetchedReferrerWallet;
      //       referred_by = ref_id;
      //     }
      
      //     const referralId = generateReferralId();
      
      //     await fastify.mysql.query(
      //       'INSERT INTO users (wallet_address, PTC_REF_ID, referred_by, referrer_address) VALUES (?, ?, ?, ?)',
      //       [wallet_address, referralId, referred_by, referrer_wallet]
      //     );
      
      //     return reply.code(200).send({
      //       status: true,
      //       msg: 'New user registered',
      //       userData: {
      //         wallet_address,
      //         PTC_REF_ID: referralId,
      //         referred_by,
      //         referrer_address: referrer_wallet
      //       }
      //     });
      
      //   } catch (err) {
      //     console.error(err);
      //     return reply.code(500).send({
      //       status: false,
      //       msg: 'Something went wrong while registering user'
      //     });
      //   }
      // });
      
      fastify.get('/RegisterNewUser', {
  schema: {
    querystring: {
      type: 'object',
      required: ['wallet_address'],
      properties: {
        wallet_address: { type: 'string' },
        ref_id: { type: 'string' }
      }
    }
  }
}, async (request, reply) => {
  try {
    const { wallet_address, ref_id } = request.query;
    
    if (!wallet_address) {
      return reply.code(400).send({
        status: false,
        msg: 'Wallet address is required'
      });
    }
    
    const ownerWallet = process.env.OWNER_WALLET ?? '0x5FcC2fA3a76599f6e672da59CBDC0a37859CD732';
    const normalizedOwnerWallet = (ownerWallet && typeof ownerWallet === 'string') 
      ? ownerWallet.toLowerCase() 
      : '';

    const existingUsers = await fastify.mysql.query(
      'SELECT * FROM users WHERE wallet_address = ?',
      [wallet_address]
    );

    if (existingUsers.length > 0) {
      let user = existingUsers[0];
      console.log(user, 'existing user');

      // Fix data inconsistency: If referred_by has a referral ID but referrer_address doesn't match, fix it
      if (user.referred_by && user.referred_by !== null && user.referred_by !== 'ADMIN') {
        const referrerUser = await fastify.mysql.query(
          'SELECT * FROM users WHERE PTC_REF_ID = ?',
          [user.referred_by]
        );
        
        if (referrerUser.length > 0) {
          const correctReferrerAddress = referrerUser[0].wallet_address;
          // If referrer_address doesn't match the referral ID's wallet, update it
          if (!user.referrer_address || user.referrer_address.toLowerCase() !== correctReferrerAddress.toLowerCase()) {
            console.log('Fixing data inconsistency for user:', wallet_address, 'Updating referrer_address from', user.referrer_address, 'to', correctReferrerAddress);
            await fastify.mysql.query(
              'UPDATE users SET referrer_address = ? WHERE wallet_address = ?',
              [correctReferrerAddress, wallet_address]
            );
            // Update user object for response
            user.referrer_address = correctReferrerAddress;
          }
        }
      }

      // Check if user has made any previous purchases
      const previousPurchases = await fastify.mysql.query(
        'SELECT COUNT(*) as purchase_count FROM ico_purchases WHERE address = ? AND status = ?',
        [wallet_address, 'success']
      );
      const hasPreviousPurchases = previousPurchases[0]?.purchase_count > 0;

      // Check if user currently has owner's referral (default/incorrect assignment)
      // Get owner's referral ID by checking if referrer_address matches owner wallet
      const hasOwnerReferral = user.referrer_address && normalizedOwnerWallet &&
        user.referrer_address.toLowerCase() === normalizedOwnerWallet;
      
      // Check if user already has a referrer (has used a referral link before)
      const hasExistingReferrer = user.referred_by && user.referred_by !== null;

      // Special case: If user has owner's referral but visits with a different referral link
      // AND has no purchases, allow updating to correct referrer
      if (ref_id && hasOwnerReferral && !hasPreviousPurchases) {
        // Validate the new referral ID
        const refData = await fastify.mysql.query(
          'SELECT * FROM users WHERE PTC_REF_ID = ?',
          [ref_id]
        );

        if (refData.length === 0) {
          return reply.code(400).send({
            status: false,
            msg: 'Invalid referral ID'
          });
        }

        const fetchedReferrerWallet = refData[0].wallet_address;

        // Don't allow self-referral
        if (fetchedReferrerWallet.toLowerCase() === wallet_address.toLowerCase()) {
          return reply.code(400).send({
            status: false,
            msg: 'You cannot refer yourself'
          });
        }

        // Don't allow updating to owner's referral if already has owner's referral
        if (fetchedReferrerWallet.toLowerCase() === normalizedOwnerWallet) {
          return reply.code(200).send({
            status: true,
            msg: 'User already saved',
            userData: user,
            hasPreviousPurchases: hasPreviousPurchases
          });
        }

        // Update to correct referrer
        console.log('Correcting referrer for user:', wallet_address, 'from owner referral to:', ref_id, '(', fetchedReferrerWallet, ')');
        await fastify.mysql.query(
          'UPDATE users SET referred_by = ?, referrer_address = ? WHERE wallet_address = ?',
          [ref_id, fetchedReferrerWallet, wallet_address]
        );

        const updatedUser = await fastify.mysql.query(
          'SELECT * FROM users WHERE wallet_address = ?',
          [wallet_address]
        );

        return reply.code(200).send({
          status: true,
          msg: 'Referral information corrected successfully',
          userData: updatedUser[0],
          hasPreviousPurchases: false
        });
      }

      // If user already has a referrer (not owner/default), block any referral link usage
      if (hasExistingReferrer && !hasOwnerReferral) {
        if (ref_id) {
          return reply.code(400).send({
            status: false,
            msg: 'Referral URL already used. You can only use a referral link during first-time registration.'
          });
        }
        // User exists with referrer, no referral link - allow normal site access
        return reply.code(200).send({
          status: true,
          msg: 'User already saved',
          userData: user,
          hasPreviousPurchases: hasPreviousPurchases
        });
      }

      // If user exists with NULL referrer, has NO purchases, and referral link is provided
      // Allow updating the referrer (user was likely auto-created by another endpoint)
      if (ref_id && !hasPreviousPurchases && (!user.referred_by || user.referred_by === null)) {
        console.log('Updating referrer for user:', wallet_address, 'from NULL to:', ref_id);
        
        // Validate referral ID
        const refData = await fastify.mysql.query(
          'SELECT * FROM users WHERE PTC_REF_ID = ?',
          [ref_id]
        );

        if (refData.length === 0) {
          console.log('Invalid referral ID:', ref_id);
          return reply.code(400).send({
            status: false,
            msg: 'Invalid referral ID'
          });
        }

        const fetchedReferrerWallet = refData[0].wallet_address;
        console.log('Referrer wallet found:', fetchedReferrerWallet);

        if (fetchedReferrerWallet.toLowerCase() === wallet_address.toLowerCase()) {
          return reply.code(400).send({
            status: false,
            msg: 'You cannot refer yourself'
          });
        }

        // Update user with referral information
        await fastify.mysql.query(
          'UPDATE users SET referred_by = ?, referrer_address = ? WHERE wallet_address = ?',
          [ref_id, fetchedReferrerWallet, wallet_address]
        );

        console.log('User referrer updated successfully:', wallet_address, '->', ref_id, '(', fetchedReferrerWallet, ')');

        const updatedUser = await fastify.mysql.query(
          'SELECT * FROM users WHERE wallet_address = ?',
          [wallet_address]
        );

        return reply.code(200).send({
          status: true,
          msg: 'Referral information updated successfully',
          userData: updatedUser[0],
          hasPreviousPurchases: false
        });
      }

      // If ref_id provided but user has purchases, block it
      if (ref_id) {
        return reply.code(400).send({
          status: false,
          msg: 'Referral URL already used. You can only use a referral link during first-time registration.'
        });
      }

      // User exists but no referral link - allow normal site access
      return reply.code(200).send({
        status: true,
        msg: 'User already saved',
        userData: user,
        hasPreviousPurchases: hasPreviousPurchases
      });
    }

    // ================================
    // Register New User
    // ================================
    // Double-check that user doesn't exist (race condition protection)
    const doubleCheckUser = await fastify.mysql.query(
      'SELECT * FROM users WHERE wallet_address = ?',
      [wallet_address]
    );

    if (doubleCheckUser.length > 0) {
      let existingUser = doubleCheckUser[0];
      
      // Fix data inconsistency: If referred_by has a referral ID but referrer_address doesn't match, fix it
      if (existingUser.referred_by && existingUser.referred_by !== null && existingUser.referred_by !== 'ADMIN') {
        const referrerUser = await fastify.mysql.query(
          'SELECT * FROM users WHERE PTC_REF_ID = ?',
          [existingUser.referred_by]
        );
        
        if (referrerUser.length > 0) {
          const correctReferrerAddress = referrerUser[0].wallet_address;
          // If referrer_address doesn't match the referral ID's wallet, update it
          if (!existingUser.referrer_address || existingUser.referrer_address.toLowerCase() !== correctReferrerAddress.toLowerCase()) {
            console.log('Fixing data inconsistency for user (double-check):', wallet_address, 'Updating referrer_address from', existingUser.referrer_address, 'to', correctReferrerAddress);
            await fastify.mysql.query(
              'UPDATE users SET referrer_address = ? WHERE wallet_address = ?',
              [correctReferrerAddress, wallet_address]
            );
            // Update existingUser object for response
            existingUser.referrer_address = correctReferrerAddress;
          }
        }
      }
      
      // Check if user has made any previous purchases
      const previousPurchases = await fastify.mysql.query(
        'SELECT COUNT(*) as purchase_count FROM ico_purchases WHERE address = ? AND status = ?',
        [wallet_address, 'success']
      );
      const hasPreviousPurchases = previousPurchases[0]?.purchase_count > 0;

      // Check if user currently has owner's referral (default/incorrect assignment)
      const hasOwnerReferral = existingUser.referrer_address && normalizedOwnerWallet &&
        existingUser.referrer_address.toLowerCase() === normalizedOwnerWallet;
      
      // Check if user already has a referrer (has used a referral link before)
      const hasExistingReferrer = existingUser.referred_by && existingUser.referred_by !== null;

      // Special case: If user has owner's referral but visits with a different referral link
      // AND has no purchases, allow updating to correct referrer
      if (ref_id && hasOwnerReferral && !hasPreviousPurchases) {
        // Validate the new referral ID
        const refData = await fastify.mysql.query(
          'SELECT * FROM users WHERE PTC_REF_ID = ?',
          [ref_id]
        );

        if (refData.length === 0) {
          return reply.code(400).send({
            status: false,
            msg: 'Invalid referral ID'
          });
        }

        const fetchedReferrerWallet = refData[0].wallet_address;

        // Don't allow self-referral
        if (fetchedReferrerWallet.toLowerCase() === wallet_address.toLowerCase()) {
          return reply.code(400).send({
            status: false,
            msg: 'You cannot refer yourself'
          });
        }

        // Don't allow updating to owner's referral if already has owner's referral
        if (fetchedReferrerWallet.toLowerCase() === normalizedOwnerWallet) {
          return reply.code(200).send({
            status: true,
            msg: 'User already saved',
            userData: existingUser,
            hasPreviousPurchases: hasPreviousPurchases
          });
        }

        // Update to correct referrer
        console.log('Double-check: Correcting referrer for user:', wallet_address, 'from owner referral to:', ref_id, '(', fetchedReferrerWallet, ')');
        await fastify.mysql.query(
          'UPDATE users SET referred_by = ?, referrer_address = ? WHERE wallet_address = ?',
          [ref_id, fetchedReferrerWallet, wallet_address]
        );

        const updatedUser = await fastify.mysql.query(
          'SELECT * FROM users WHERE wallet_address = ?',
          [wallet_address]
        );

        return reply.code(200).send({
          status: true,
          msg: 'Referral information corrected successfully',
          userData: updatedUser[0],
          hasPreviousPurchases: false
        });
      }

      // If user already has a referrer (not owner/default), block any referral link usage
      if (hasExistingReferrer && !hasOwnerReferral) {
        if (ref_id) {
          return reply.code(400).send({
            status: false,
            msg: 'Referral URL already used. You can only use a referral link during first-time registration.'
          });
        }
        return reply.code(200).send({
          status: true,
          msg: 'User already saved',
          userData: existingUser,
          hasPreviousPurchases: hasPreviousPurchases
        });
      }

      // If user exists with NULL referrer, has NO purchases, and referral link is provided
      // Allow updating the referrer (user was likely auto-created by another endpoint)
      if (ref_id && !hasPreviousPurchases && (!existingUser.referred_by || existingUser.referred_by === null)) {
        console.log('Double-check: Updating referrer for user:', wallet_address, 'from NULL to:', ref_id);
        
        // Validate referral ID
        const refData = await fastify.mysql.query(
          'SELECT * FROM users WHERE PTC_REF_ID = ?',
          [ref_id]
        );

        if (refData.length === 0) {
          console.log('Double-check: Invalid referral ID:', ref_id);
          return reply.code(400).send({
            status: false,
            msg: 'Invalid referral ID'
          });
        }

        const fetchedReferrerWallet = refData[0].wallet_address;
        console.log('Double-check: Referrer wallet found:', fetchedReferrerWallet);

        if (fetchedReferrerWallet.toLowerCase() === wallet_address.toLowerCase()) {
          return reply.code(400).send({
            status: false,
            msg: 'You cannot refer yourself'
          });
        }

        // Update user with referral information
        await fastify.mysql.query(
          'UPDATE users SET referred_by = ?, referrer_address = ? WHERE wallet_address = ?',
          [ref_id, fetchedReferrerWallet, wallet_address]
        );

        console.log('Double-check: User referrer updated successfully:', wallet_address, '->', ref_id, '(', fetchedReferrerWallet, ')');

        const updatedUser = await fastify.mysql.query(
          'SELECT * FROM users WHERE wallet_address = ?',
          [wallet_address]
        );

        return reply.code(200).send({
          status: true,
          msg: 'Referral information updated successfully',
          userData: updatedUser[0],
          hasPreviousPurchases: false
        });
      }

      // If ref_id provided but user has purchases, block it
      if (ref_id) {
        return reply.code(400).send({
          status: false,
          msg: 'Referral URL already used. You can only use a referral link during first-time registration.'
        });
      }

      // User exists but no referral link - return existing user
      return reply.code(200).send({
        status: true,
        msg: 'User already saved',
        userData: existingUser,
        hasPreviousPurchases: hasPreviousPurchases
      });
    }

    // Default to NULL - no referrer unless referral link is provided
    let referred_by = null;
    let referrer_wallet = null;

    if (ref_id) {
      const refData = await fastify.mysql.query(
        'SELECT * FROM users WHERE PTC_REF_ID = ?',
        [ref_id]
      );

      if (refData.length === 0) {
        return reply.code(400).send({
          status: false,
          msg: 'Invalid referral ID'
        });
      }

      const fetchedReferrerWallet = refData[0].wallet_address;

      if (fetchedReferrerWallet.toLowerCase() == wallet_address.toLowerCase()) {
        return reply.code(400).send({
          status: false,
          msg: 'You cannot refer yourself'
        });
      }

      referrer_wallet = fetchedReferrerWallet;
      referred_by = ref_id;
    }

    const referralId = generateReferralId();

    // Insert user - handle NULL referrer_address if database doesn't support it yet
    try {
      await fastify.mysql.query(
        'INSERT INTO users (wallet_address, PTC_REF_ID, referred_by, referrer_address) VALUES (?, ?, ?, ?)',
        [wallet_address, referralId, referred_by, referrer_wallet]
      );
    } catch (insertError) {
      // If error is due to NULL referrer_address, try with empty string or handle gracefully
      if (insertError.code === 'ER_BAD_NULL_ERROR' && insertError.sqlMessage?.includes('referrer_address')) {
        console.log('Database does not allow NULL referrer_address, using empty string instead');
        // Try with empty string as fallback
        await fastify.mysql.query(
          'INSERT INTO users (wallet_address, PTC_REF_ID, referred_by, referrer_address) VALUES (?, ?, ?, ?)',
          [wallet_address, referralId, referred_by, referrer_wallet || '']
        );
      } else {
        throw insertError; // Re-throw if it's a different error
      }
    }

    // New users have no previous purchases
    return reply.code(200).send({
      status: true,
      msg: 'New user registered',
      userData: {
        wallet_address,
        PTC_REF_ID: referralId,
        referred_by,
        referrer_address: referrer_wallet
      },
      hasPreviousPurchases: false
    });

  } catch (err) {
    console.error('Error in RegisterNewUser:', err);
    console.error('Error stack:', err.stack);
    console.error('Error details:', {
      message: err.message,
      code: err.code,
      sqlMessage: err.sqlMessage,
      sqlState: err.sqlState
    });
    return reply.code(500).send({
      status: false,
      msg: 'Something went wrong while registering user',
      error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  }
});

      
    async function splitSign(hash, nonce) {
        var signature = ethers.utils.splitSignature(hash);
        return [signature.v, signature.r, signature.s, nonce];
    }
    
    fastify.post('/createSign', {
      schema: {
          body: {
              type: 'object',
              required: ['index', 'address', 'caller', 'amount'],
              properties: {
                  index: { type: "integer" },
                  address: { type: "string" },
                  caller: { type: "string" },
                  amount: { type: "string" }
              }
          }
      }
  }, async (req, reply) => {
      const { index, address, caller, amount } = req.body;
      try {
        console.log('Request body:', req.body);          
        var getprovider = 'https://eth-sepolia.g.alchemy.com/v2/9uqAUaY1a3NUf3K0HvW6GdjVv4SoM7Z8'
        var provider = new ethers.providers.JsonRpcProvider(getprovider);
        let wallet = new ethers.Wallet(process.env.SIGN_KEY, provider);
        console.log(wallet,'wallet')
        var nonce = Math.floor(new Date().getTime() / 1000);
        hash = ethers.utils.solidityKeccak256(["uint256", "address", "address", "uint256", 'uint256'], [index, address, caller, amount.toString(), nonce])
        var msgHash = ethers.utils.arrayify(hash);
        var hash = await wallet.signMessage(msgHash);
        var sign = await splitSign(hash, nonce);   
        console.log(sign,'sign');       
        return reply.code(200).send({ status: true, signature: sign });
      }
      catch (error) {
          console.log('Signature generation error:', error);
          return reply.code(500).send({ status: false, message: "Unable to Generate Signature" });
      }
  });
  fastify.post('/createSignClaim', {
    schema: {
        body: {
            type: 'object',
            required: ['address', 'caller', 'amount'],
            properties: {
                address: { type: "string" },
                caller: { type: "string" },
                amount: { type: "string" }
            }
        }
    }
}, async (req, reply) => {
    const { address, caller, amount } = req.body;
    try {
      console.log('Request body:', req.body);          
      var getprovider = 'https://eth-sepolia-testnet.api.pocket.network'
      var provider = new ethers.providers.JsonRpcProvider(getprovider);
      let wallet = new ethers.Wallet(process.env.SIGN_KEY, provider);
      console.log(wallet,'wallet')
      var nonce = Math.floor(new Date().getTime() / 1000);
      const amounts = ethers.utils.parseUnits(amount.toString(), 18);
      hash = ethers.utils.solidityKeccak256(["address", "address","uint256", 'uint256'], [caller, address, amounts, nonce])
      var msgHash = ethers.utils.arrayify(hash);
      var hash = await wallet.signMessage(msgHash);
      var sign = await splitSign(hash, nonce);   
      console.log(sign,'sign');       
      return reply.code(200).send({ status: true, signature: sign });
    }
    catch (error) {
        console.log('Signature generation error:', error);
        return reply.code(500).send({ status: false, message: "Unable to Generate Signature" });
    }
});
  
  fastify.post('/savePurchases', {
    schema: {
      body: {
        type: "object",
        required: [
          'address',
          'CryptoValue',
          'payment_type',
          'PTC_tokens',
          'transHash',
          'USDvalue_of_crypto_purchased',
        ],
        properties: {
          address: { type: "string" },
          CryptoValue: { type: "string" },
          payment_type: { type: "string" },
          PTC_tokens: { type: "string" },
          transHash: { type: "string", minLength: 64 },
          USDvalue_of_crypto_purchased: { type: "string" },
          sale_type: { type: "string" },
          referrer_bonus: { type: "string" },
          referrer_address: { type: "string" },
        }
      }
    }
  }, async (req, reply) => {
    try{
    const {
      address,
      CryptoValue,
      payment_type,
      PTC_tokens,
      USDvalue_of_crypto_purchased,
      transHash,
      sale_type = '',
      referrer_bonus = '0',
      referrer_address = ''
    } = req.body;
  
    console.log('Purchase Data:', req.body);

    // Check if transaction hash already exists (prevent duplicates)
    const existingTransaction = await fastify.mysql.query(
      'SELECT * FROM ico_purchases WHERE trans_hash = ?',
      [transHash]
    );

    if (existingTransaction.length > 0) {
      console.log('Transaction already exists:', transHash);
      return reply.code(200).send({
        status: true,
        msg: `${PTC_tokens} PTC tokens purchased successfully!!! (Already recorded)`,
      });
    }
  
    // Insert purchase record
    const insertRows = await fastify.mysql.query(
      `INSERT INTO ico_purchases
        (address, crypto_value, payment_type, ptc_tokens, trans_hash, usd_value_of_crypto, sale_type, status, referrer_bonus, referrer_address)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [address, CryptoValue, payment_type, PTC_tokens, transHash, USDvalue_of_crypto_purchased, sale_type, 'success', referrer_bonus, referrer_address]
    );

    console.log('Purchase inserted successfully:', insertRows.insertId);

    // If sale_type was not provided, try to resolve and attach the current active sale type
    try {
      if (!sale_type || String(sale_type).trim() === '') {
        const activeRows = await fastify.mysql.query(`
          SELECT * FROM token_sales
          WHERE status = 'active' OR (NOW() BETWEEN start_at AND end_at)
          ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END, start_at DESC
          LIMIT 1
        `);
        const active = Array.isArray(activeRows) ? (activeRows[0] || null) : (activeRows || null);
        if (active && (active.type || active.sale_type)) {
          const resolvedType = active.type || active.sale_type || '';
          await fastify.mysql.query('UPDATE ico_purchases SET sale_type = ? WHERE id = ?', [resolvedType, insertRows.insertId]);
          console.log('Attached active sale_type to purchase:', resolvedType);
        }
      }
    } catch (e) {
      fastify.log && fastify.log.warn && fastify.log.warn('Failed to attach sale_type to purchase:', e.message || e);
    }
  
    // Get user data to process referral claims
    const users = await fastify.mysql.query('SELECT * FROM users WHERE wallet_address = ?', [address]);

    if (users.length > 0) {
      const user = users[0];
      const normalizedReferrer = user.referrer_address ? user.referrer_address.toLowerCase() : null;
      const ownerWallet = (process.env.OWNER_WALLET ?? '').toLowerCase();
      // Check if user has a valid referrer (not NULL)
      const hasValidReferrer = user.referred_by && user.referred_by !== null && normalizedReferrer;
      const claimedAmount = parseFloat(referrer_bonus ?? '0');

      if (hasValidReferrer && claimedAmount > 0) {
        const refClaimCheck = await fastify.mysql.query(
          `SELECT * FROM referral_claims WHERE referrer_id = ? AND referred_user_id = ?`,
          [normalizedReferrer, address]
        );

        if (refClaimCheck.length === 0) {
          await fastify.mysql.query(
            `INSERT INTO referral_claims (referrer_id, referred_user_id, amount, status, created_at, updated_at)
             VALUES (?, ?, ?, 'Pending', NOW(), NOW())`,
            [normalizedReferrer, address, claimedAmount.toFixed(2)]
          );

          console.log(`Referral claim created for ${user.referrer_address} => ${claimedAmount.toFixed(2)} PTC`);
        } else {
          console.log(`Referral claim already exists for ${user.referrer_address}`);
        }
      }
    } else {
      console.warn(`User not found in database for address: ${address}. Purchase saved but referral claim skipped.`);
    }
  
    return reply.code(200).send({
      status: true,
      msg: `${PTC_tokens} PTC tokens purchased successfully!!!`,
    });
  }catch(err){
    console.error('Error saving purchase:', err);
    // Check if it's a duplicate key error
    if (err.code === 'ER_DUP_ENTRY' || err.errno === 1062) {
      return reply.code(200).send({
        status: true,
        msg: 'Transaction already recorded',
      });
    }
    return reply.code(500).send({ 
      status: false, 
      msg: 'Internal Server Error',
      error: err.message 
    });
  }
  });
  
  fastify.get('/getUserData', {
    schema: {
      querystring: {
        type: 'object',
        required: ['address'],
        properties: {
          address: { type: 'string' },
        }
      }
    }
  }, async (request, reply) => {
    const { address} = request.query;
    console.log(request.query)
    try {
      let checkData = await fastify.mysql.query('SELECT * FROM users WHERE wallet_address = ?', [address]);
      console.log(checkData,'check data')
      if (checkData.length > 0) {
        // Fix data inconsistency: If referred_by has a referral ID but referrer_address doesn't match, fix it
        const user = checkData[0];
        if (user.referred_by && user.referred_by !== null && user.referred_by !== 'ADMIN') {
          const referrerUser = await fastify.mysql.query(
            'SELECT * FROM users WHERE PTC_REF_ID = ?',
            [user.referred_by]
          );
          
          if (referrerUser.length > 0) {
            const correctReferrerAddress = referrerUser[0].wallet_address;
            // If referrer_address doesn't match the referral ID's wallet, update it
            if (!user.referrer_address || user.referrer_address.toLowerCase() !== correctReferrerAddress.toLowerCase()) {
              console.log('Fixing data inconsistency for user:', address, 'Updating referrer_address from', user.referrer_address, 'to', correctReferrerAddress);
              await fastify.mysql.query(
                'UPDATE users SET referrer_address = ? WHERE wallet_address = ?',
                [correctReferrerAddress, address]
              );
              // Update checkData for response
              checkData[0].referrer_address = correctReferrerAddress;
            }
          }
        }

        const referrals = await fastify.mysql.query('select * from users where referred_by = ?',[checkData[0].PTC_REF_ID]);
        const transactions = await fastify.mysql.query('SELECT * FROM ico_purchases WHERE address = ?', [address]);
        const basictransactions = await fastify.mysql.query('SELECT * FROM ico_purchases WHERE referrer_address = ? AND referrer_bonus > 0', [address]);
        const txNorm = Array.isArray(transactions) ? transactions.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at) })) : transactions;
        const basicTxNorm = Array.isArray(basictransactions) ? basictransactions.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at) })) : basictransactions;
        return reply.code(200).send({
          status: true,
          UserData: checkData,
          transactions: txNorm,
          referrals:referrals,
          basictransactions:basicTxNorm
        });
      } else {
        // Don't auto-create users here - user creation should only happen via RegisterNewUser endpoint
        // This ensures referral links are processed correctly
        return reply.code(200).send({
          status: true,
          msg: "User not found. Please register first.",
          UserData: [],
          transactions: [],
          referrals:[],
          basictransactions:[]
        });
      }
  
    } catch (err) {
      console.error(err);
      return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  });


  fastify.get('/getRefferalData', {
    schema: {
      querystring: {
        type: 'object',
        required: ['address'],
        properties: {
          address: { type: 'string' },
          
        }
      }
    }
  }, async (request, reply) => {
    const { address} = request.query;
    console.log(request.query)
    try {
      let checkData = await fastify.mysql.query('SELECT * FROM users WHERE wallet_address = ?', [address]);
      console.log(checkData,'check data')
      if (checkData.length > 0) {
        // Fix data inconsistency: If referred_by has a referral ID but referrer_address doesn't match, fix it
        const user = checkData[0];
        if (user.referred_by && user.referred_by !== null && user.referred_by !== 'ADMIN') {
          const referrerUser = await fastify.mysql.query(
            'SELECT * FROM users WHERE PTC_REF_ID = ?',
            [user.referred_by]
          );
          
          if (referrerUser.length > 0) {
            const correctReferrerAddress = referrerUser[0].wallet_address;
            // If referrer_address doesn't match the referral ID's wallet, update it
            if (!user.referrer_address || user.referrer_address.toLowerCase() !== correctReferrerAddress.toLowerCase()) {
              console.log('Fixing data inconsistency for user:', address, 'Updating referrer_address from', user.referrer_address, 'to', correctReferrerAddress);
              await fastify.mysql.query(
                'UPDATE users SET referrer_address = ? WHERE wallet_address = ?',
                [correctReferrerAddress, address]
              );
              // Update checkData for response
              checkData[0].referrer_address = correctReferrerAddress;
            }
          }
        }

        const referrals = await fastify.mysql.query('select * from users where referred_by = ?',[checkData[0].PTC_REF_ID]);
        const basictransactions = await fastify.mysql.query('SELECT * FROM ico_purchases WHERE address = ?', [address]);
        const transactions = await fastify.mysql.query('SELECT * FROM ico_purchases WHERE referrer_address = ? AND referrer_bonus > 0', [address]);
        const basicTxNorm2 = Array.isArray(basictransactions) ? basictransactions.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at) })) : basictransactions;
        const txNorm2 = Array.isArray(transactions) ? transactions.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at) })) : transactions;
        return reply.code(200).send({
          status: true,
          UserData: checkData,
          transactions: txNorm2,
          referrals:referrals,
          basictransactions:basicTxNorm2
        });
      } else {
        // Don't auto-create users here - user creation should only happen via RegisterNewUser endpoint
        // This ensures referral links are processed correctly
        return reply.code(200).send({
          status: true,
          msg: "User not found. Please register first.",
          UserData: [],
          transactions: [],
          referrals:[],
          basictransactions:[]
        });
      }
  
    } catch (err) {
      console.error(err);
      return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  });

  fastify.get('/getTransactionDetails',{
    schema:{
      query:{
        type:"object",
        required:['address'],
        properties:{
          query:{
            type:"string"
          }
        }
      }
    }
  }, async (request, reply) => {
    try {
      const results = await fastify.mysql.query(`
        SELECT * from ico_purchases where address=?
      `,[request.query.address]);
      const normalized = Array.isArray(results) ? results.map((r) => ({ ...r, created_at_utc: toUtcISOString(r.created_at) })) : results;
      const getReferrals = await fastify.mysql.query(`
        SELECT * FROM referral_claims 
        WHERE referrer_id = ? 
          AND status IN ('Pending', 'Success')
      `, [request.query.address]);
      const [referralLink] = await fastify.mysql.query(`
        SELECT * FROM users 
        WHERE wallet_address = ? 
      `, [request.query.address]);
      return reply.code(200).send({
        status: true,
        userData: normalized,
        referralData:getReferrals,
        referralLink:referralLink?.PTC_REF_ID
      });
    } catch (err) {
      console.error(err);
      return reply.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  });
  
  fastify.get('/getreferralDetails',{
    schema:{
      query:{
        type:"object",
        required:['address'],
        properties:{
          query:{
            type:"string"
          }
        }
      }
    }
  },async(req,res)=>{
    try{
      let userData = await fastify.mysql.query(`
        SELECT * FROM users 
        WHERE wallet_address = ? 
      `, [req.query.address]);
      
      // Fix data inconsistency: If referred_by has a referral ID but referrer_address doesn't match, fix it
      if (userData.length > 0) {
        const user = userData[0];
        if (user.referred_by && user.referred_by !== null && user.referred_by !== 'ADMIN') {
          const referrerUser = await fastify.mysql.query(
            'SELECT * FROM users WHERE PTC_REF_ID = ?',
            [user.referred_by]
          );
          
          if (referrerUser.length > 0) {
            const correctReferrerAddress = referrerUser[0].wallet_address;
            // If referrer_address doesn't match the referral ID's wallet, update it
            if (!user.referrer_address || user.referrer_address.toLowerCase() !== correctReferrerAddress.toLowerCase()) {
              console.log('Fixing data inconsistency for user:', req.query.address, 'Updating referrer_address from', user.referrer_address, 'to', correctReferrerAddress);
              await fastify.mysql.query(
                'UPDATE users SET referrer_address = ? WHERE wallet_address = ?',
                [correctReferrerAddress, req.query.address]
              );
              // Update userData for response
              userData[0].referrer_address = correctReferrerAddress;
            }
          }
        }
      }

      const getReferrals = await fastify.mysql.query(`
        SELECT * FROM referral_claims 
        WHERE referrer_id = ? 
      `, [req.query.address]);

      const tree = await fastify.mysql.query(`
        SELECT * FROM users 
        WHERE referrer_address = ? 
      `, [req.query.address]);

      // Get distinct referred users who made successful purchases (counts only when referred user actually bought)
      // Use case-insensitive match to avoid mismatch due to checksum casing
      const purchasedReferrals = await fastify.mysql.query(
        `SELECT DISTINCT LOWER(address) as referred_address FROM ico_purchases WHERE LOWER(referrer_address) = LOWER(?) AND status = 'success'`,
        [req.query.address]
      );

      // Ensure referredCount is an integer
      const referredCount = Array.isArray(purchasedReferrals) ? purchasedReferrals.length : 0;

      return res.code(200).send({
        status: true,
        referralData: getReferrals,
        treeData: tree,
        userData,
        referredPurchases: purchasedReferrals,
        referredCount
      });
    } catch (err) {
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })

  fastify.post('/syncStakeRecord',{
    schema:{
      body:{
        type:'object',
        required:['address','stake_id','plan_id','amount','rewardfee','stake_time','end_time','start_time','rewardAmount','stake_hash'],
        properties:{
          address:{type:'string'},
          stake_id:{type:'number'},
          plan_id:{type:'number'}
        }
      }
    }
  },async(req,res)=>{
    try{
      const { address, stake_id, plan_id, amount, rewardfee, stake_time, end_time, start_time, rewardAmount, stake_hash, withdraw_status, withdraw_hash } = req.body;

      const existing = await fastify.mysql.query(
        'SELECT * FROM stakes WHERE wallet_address = ? AND stake_id = ?',[address, stake_id]
      );

      const validTill = formatUnixToMysql(end_time);

      if(existing.length > 0){
        const current = existing[0];
        if (withdraw_status && current.withdraw_status !== withdraw_status) {
          await fastify.mysql.query(
            'UPDATE stakes SET withdraw_status = ?, withdraw_hash = COALESCE(?, withdraw_hash), rewardAmount = COALESCE(?, rewardAmount) WHERE wallet_address = ? AND stake_id = ?',
            [withdraw_status, withdraw_hash || current.withdraw_hash, rewardAmount ?? current.rewardAmount, address, stake_id]
          );
        }
        return res.send({
          status:true,
          msg:'Stake already synced',
          data:existing
        });
      }

      const insert = await fastify.mysql.query(
        'INSERT INTO stakes (stake_id, plan_id, wallet_address, amount, stake_hash, rewardfee, stake_time, stake_status, valid_till, rewardAmount, withdraw_status, withdraw_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
          stake_id,
          plan_id,
          address,
          amount,
          stake_hash,
          rewardfee,
          stake_time,
          'SUCCESS',
          validTill,
          rewardAmount,
          withdraw_status || 'PENDING',
          withdraw_hash || null,
          formatUnixToMysql(start_time)
        ]
      );

      return res.send({
        status:true,
        msg:'Stake synced successfully',
        data:insert
      });
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })
  fastify.post('/stakePTC',{
    schema:{
      body:{
        type:"object",
        required:['address','amount','plan_id','transHash','duration','reward','stakeId'],
        properties:{
          address:{
            type:"string"
          }
        }
      }
    }
  },async(req,res)=>{
      try {
        const { address, amount, plan_id, transHash, duration, reward, stakeId } = req.body;

        const duplicateStake = await fastify.mysql.query(
          'SELECT id FROM stakes WHERE wallet_address = ? AND stake_id = ?',
          [address, stakeId]
        );

        if (duplicateStake.length > 0) {
          return res.code(409).send({
            status: false,
            msg: 'Stake already recorded for this stakeId'
          });
        }

        const endTime = addMinutesToCurrentTime(duration);
        const calculaterwards = parseFloat(amount) * parseFloat(reward/100);
        const insertData = await fastify.mysql.query(
            'INSERT INTO stakes (stake_id, plan_id, wallet_address, amount, stake_hash, rewardfee, stake_time, stake_status, valid_till,rewardAmount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)',
            [stakeId, plan_id, address, amount, transHash, reward, duration, 'SUCCESS', endTime,calculaterwards]
        );
        return res.send({
            status: true,
            msg: 'Stake created successfully',
            stakeId,
            data: insertData 
        });
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })
  function addMinutesToCurrentTime(minutes) {
    const nowUTC = new Date();

  
  const istTime = new Date(nowUTC.getTime() + minutes * 60 * 1000 + 5.5 * 60 * 60 * 1000);

  const yyyy = istTime.getUTCFullYear();
  const mm = String(istTime.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(istTime.getUTCDate()).padStart(2, '0');
  const hh = String(istTime.getUTCHours()).padStart(2, '0');
  const min = String(istTime.getUTCMinutes()).padStart(2, '0');
  const ss = String(istTime.getUTCSeconds()).padStart(2, '0');
  const ms = String(istTime.getUTCMilliseconds()).padStart(3, '0');

  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}.${ms}`;
  }

  function formatUnixToMysql(seconds) {
    if (!seconds) {
      const now = new Date();
      const yyyy = now.getUTCFullYear();
      const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(now.getUTCDate()).padStart(2, '0');
      const hh = String(now.getUTCHours()).padStart(2, '0');
      const min = String(now.getUTCMinutes()).padStart(2, '0');
      const ss = String(now.getUTCSeconds()).padStart(2, '0');
      const ms = String(now.getUTCMilliseconds()).padStart(3, '0');
      return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}.${ms}`;
    }

    const date = new Date(Number(seconds) * 1000);
    const yyyy = date.getUTCFullYear();
    const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(date.getUTCDate()).padStart(2, '0');
    const hh = String(date.getUTCHours()).padStart(2, '0');
    const min = String(date.getUTCMinutes()).padStart(2, '0');
    const ss = String(date.getUTCSeconds()).padStart(2, '0');
    const ms = String(date.getUTCMilliseconds()).padStart(3, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}.${ms}`;
  }

  fastify.get('/checkStake',{
    schema:{
      query:{
        type:"object",
        required:['address','plan_id']
      }
    }
  },async(req,res)=>{
    try{
      const{address,plan_id} = req.query;
      const activeStakes = await fastify.mysql.query(
        'SELECT COUNT(*) as stakeCount FROM stakes WHERE withdraw_status != ? AND plan_id = ? AND wallet_address = ?',
        ['SUCCESS', plan_id, address]
    );
    return res.send({
        status: true,
        stakeCount: activeStakes[0]?.stakeCount || 0,
        msg: 'Stake count fetched successfully'
    });
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })

  fastify.get('/getStakeHistory',{
    schema:{
      query:{
        type:"object",
        required:['address']
      }
    }
  },async(req,res)=>{
    try{
      const{address} = req.query;
      // Ensure matured stakes are marked in DB before returning history
      try {
        await fastify.mysql.query(
          `UPDATE stakes SET withdraw_status = 'MATURE' 
           WHERE valid_till <= NOW() AND withdraw_status NOT IN ('SUCCESS','MATURE')`
        );
      } catch (uErr) {
        fastify.log && fastify.log.warn && fastify.log.warn('Unable to mark matured stakes in user history:', uErr.message || uErr);
      }

      const checkStaked = await fastify.mysql.query(
        'SELECT * FROM stakes WHERE  wallet_address = ?',
        [address]
    );
        const normalized = Array.isArray(checkStaked)
          ? checkStaked.map((r) => ({
              ...r,
              created_at_utc: toUtcISOString(r.created_at),
              valid_till_utc: toUtcISOString(r.valid_till)
            }))
          : checkStaked;
        return res.send({
            status: true,
            data:normalized,
            msg: `Staked Records fetched`,
        });
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })

  fastify.get('/getUserStakes',{
    schema:{
      query:{
        type:"object",
        required:['address']
      }
    }
  },async(req,res)=>{
    try{
      const { address } = req.query;
      const stakes = await fastify.mysql.query(
        'SELECT * FROM stakes WHERE wallet_address = ? ORDER BY plan_id, stake_id',
        [address]
      );
      return res.send({
        status:true,
        data:stakes,
        msg:'User stakes fetched successfully'
      });
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })

  fastify.post('/updateStake',{
    schema:{
      body:{
        type:"object",
        required:['address','plan_id','stake_id','transHash'],
        properties:{
          stake_id:{ type:'number' }
        }
      }
    }
  },async(req,res)=>{
    try{
      console.log(req.body,'stake update')
      const{address,plan_id,transHash,stake_id,rewardAmount} = req.body;
      const checkStaked = await fastify.mysql.query(
        'SELECT * FROM stakes WHERE wallet_address = ? AND plan_id = ? AND stake_id = ?',
        [address, plan_id, stake_id]
    );
    if(checkStaked.length > 0){
      const updateStakes = await fastify.mysql.query(
        'UPDATE stakes SET  withdraw_hash = ?, withdraw_status = ?, rewardAmount = COALESCE(?, rewardAmount) WHERE wallet_address = ? AND plan_id = ? AND stake_id = ?',
        [transHash, 'SUCCESS', rewardAmount ?? null, address, plan_id, stake_id]
      );
      return res.send({
        status: true,
        data:checkStaked,
        msg: `Staked Withdraw Successfull!!!`,
      });
    }else{
      return res.send({
        status: false,
        msg: `Staked Records Not found`,
    });
    }
       
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })

  // User-facing sync endpoint: query contract for user stake details and withdraw event,
  // then persist withdraw_hash, withdraw_status and rewardAmount into DB.
  fastify.post('/syncStakeTx', async (request, reply) => {
    const { address, stake_id } = request.body || {};
    if (!address || !stake_id) return reply.code(400).send({ success: false, message: 'address and stake_id are required' });

    try {
      const rows = await fastify.mysql.query('SELECT * FROM stakes WHERE wallet_address = ? AND stake_id = ?', [address, stake_id]);
      if (!rows || rows.length === 0) return reply.code(404).send({ success: false, message: 'stake not found' });
      const stake = rows[0];

      // Import client Configs to reuse ABI and contract address
      const { pathToFileURL } = require('url');
      const cfgPath = path.resolve(__dirname, '../../src/Utils/Configs.js');
      let ICO = null;
      try {
        const mod = await import(pathToFileURL(cfgPath).href);
        ICO = mod?.ICO || null;
      } catch (impErr) {
        fastify.log.error('Failed to import Configs.js for ABI/address', impErr);
      }

      if (!ICO || !ICO.STAKING_CONTRACT || !ICO.STAKING_ABI) {
        return reply.code(500).send({ success: false, message: 'Staking contract info not available' });
      }

      const ethers = require('ethers');
      const providerUrl = process.env.RPC_URL || process.env.RPC || null;
      if (!providerUrl) return reply.code(500).send({ success: false, message: 'RPC_URL not configured on server' });

      const provider = new ethers.providers.JsonRpcProvider(providerUrl);
      const contract = new ethers.Contract(ICO.STAKING_CONTRACT, ICO.STAKING_ABI, provider);

      // Call on-chain getUserDetails to get status and reward info
      let onChain = null;
      try {
        onChain = await contract.getUserDetails(address, Number(stake_id));
      } catch (err) {
        fastify.log.warn('getUserDetails call failed:', err.message || err);
      }

      // ABI: getUserDetails returns (userStruct, reward)
      let userStruct = null;
      let rewardOnChain = null;
      if (onChain) {
        if (Array.isArray(onChain)) {
          userStruct = onChain[0] || null;
          rewardOnChain = onChain[1] ?? null;
        } else if (onChain.user) {
          userStruct = onChain.user;
          rewardOnChain = onChain.reward ?? null;
        }
      }

      // If userStruct indicates withdrawn (status == false), search for Withdraw event to get txHash
      let txHash = null;
      if (userStruct && typeof userStruct.status !== 'undefined' && userStruct.status === false) {
        try {
          const latestBlock = await provider.getBlockNumber();
          const fromBlock = Math.max(0, latestBlock - 200000);
          const filter = contract.filters.Withdraw(address);
          const events = await contract.queryFilter(filter, fromBlock, latestBlock);
          if (events && events.length > 0) {
            // Prefer event after stake.valid_till if present
            let matched = null;
            if (stake.valid_till) {
              const validT = Math.floor(new Date(stake.valid_till).getTime() / 1000);
              for (let i = events.length - 1; i >= 0; i--) {
                const ev = events[i];
                try {
                  const blk = await provider.getBlock(ev.blockNumber);
                  if (blk && blk.timestamp && blk.timestamp >= validT) {
                    matched = ev;
                    break;
                  }
                } catch (be) {
                  // ignore block lookup errors
                }
              }
            }
            if (!matched) matched = events[events.length - 1];
            txHash = matched.transactionHash;
          }
        } catch (e) {
          fastify.log.warn('Error querying Withdraw events:', e.message || e);
        }
      }

      // Prepare update values
      const newWithdrawStatus = (userStruct && typeof userStruct.status !== 'undefined' && userStruct.status === false) ? 'SUCCESS' : stake.withdraw_status;
      const newReward = (rewardOnChain && rewardOnChain.toString) ? rewardOnChain.toString() : (rewardOnChain ?? stake.rewardAmount);
      const newWithdrawHash = txHash || stake.withdraw_hash || (userStruct && typeof userStruct.status !== 'undefined' && userStruct.status === false ? 'auto_synced' : null);

      // Update DB
      try {
        await fastify.mysql.query('UPDATE stakes SET withdraw_status = ?, withdraw_hash = COALESCE(?, withdraw_hash), rewardAmount = COALESCE(?, rewardAmount) WHERE wallet_address = ? AND stake_id = ?', [newWithdrawStatus, newWithdrawHash, newReward, address, stake_id]);
      } catch (uErr) {
        fastify.log.error('Failed to update stake row:', uErr.message || uErr);
      }

      const updated = await fastify.mysql.query('SELECT * FROM stakes WHERE wallet_address = ? AND stake_id = ?', [address, stake_id]);
      return reply.send({ success: true, data: updated[0], note: userStruct ? 'onchain-checked' : 'no-onchain-info' });
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ success: false, message: 'Internal error', error: err.message });
    }
  });
  fastify.post('/updateReferralClaims',{
    schema:{
      body:{
        type:"object",
        required:['transHash','id']
      }
    }
  },async(req,res)=>{
    try{
      console.log(req.body,'claim update')
      const{transHash,id} = req.body;
      const checkStaked = await fastify.mysql.query(
        'SELECT * FROM referral_claims WHERE  id = ?',
        [id]
    );
    if(checkStaked.length > 0){
      const updateStakes = await fastify.mysql.query(
        'UPDATE referral_claims SET  transaction_id = ?, status = ? WHERE id = ?',
        [transHash, 'Success', id]
      );
      return res.send({
        status: true,
        data:checkStaked,
        msg: `Claimed  Successfully!!!`,
      });
    }else{
      return res.send({
        status: false,
        msg: `claim Records Not found`,
    });
    }
       
    }catch(err){
      console.error(err);
      return res.code(500).send({ status: false, msg: 'Internal Server Error' });
    }
  })


  };
  